package codec

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// RESP 是默认 codec，仿 Redis 序列化协议（RESP2）。
//
// 与官方 RESP 的两点差异，都是为了"单条命令一次往返发送"这个用途：
//
//  1. 仅对**命令名**使用 Bulk String（`$len\r\n<name>\r\n`），参数改用 Redis 的
//     Inline Command 形态（`<arg>\r\n`，参数内的 CR/LF 以 `\r\n` 转义）。这样
//     `printf '*1\r\n$3\r\nGET\r\n\r\n' | nc host port` 之类的手测方式依然成立，
//     同时省掉每个参数的长度前缀与两次 memcpy。
//  2. 应答的状态字兼容 RESP 的 `+`/`-`/`:`/`$`/`*`，但 Redis 没有的语义
//     （unsupported / closed / not_integer / invalid_ttl / auth_required）
//     借 Redis 6 起的**属性行**（`|`）承载，见 writeAttr。
//
// 之所以不能用 go-redis 自带的 proto 包：它位于 internal/，外部模块无法引用。
//
// Client 侧连接一律处于 RESP 的"请求-应答"模式，任何不认识的字节序都属于
// 协议错误，读取侧直接报错而不会静默跳过。
type respCodec struct {
	br *bufio.Reader
	bw *bufio.Writer
}

// NewRESPCodecForTest 仅供 rpc 包内测试构造 codec。
func NewRESPCodecForTest(r io.Reader) Codec { return NewRESP(r, io.Discard) }

// NewRESP 返回默认的 RESP 编解码器。
func NewRESP(r io.Reader, w io.Writer) Codec {
	return &respCodec{
		br: bufio.NewReaderSize(r, 32*1024),
		bw: bufio.NewWriterSize(w, 32*1024),
	}
}

func (*respCodec) Name() string { return "resp" }

const crlf = "\r\n"

// ---- 请求 ----

// WriteRequest 编码一条命令：
//
//	$<cmdlen>\r\n<cmd>\r\n
//	*<argc>\r\n
//	$<len>\r\n<arg>\r\n  （重复 argc 次）
//
// 参数个数在报文头**显式给出**，而不是靠"末尾空行"隐式界定：空参数（如
// SCAN 的空 start/end）编码后与结束标志无法区分，隐式界定会直接丢掉参数。
// 每个参数都用 Bulk String 形态，因此空参数自然表达为 `$0\r\n\r\n`。
func (c *respCodec) WriteRequest(w io.Writer, args [][]byte) error {
	if len(args) == 0 {
		return fmt.Errorf("resp: empty command")
	}
	if len(args) > MaxArgs {
		return fmt.Errorf("resp: too many args: %d", len(args))
	}
	bw := c.bw
	if err := writeBulk(bw, args[0]); err != nil {
		return err
	}
	if err := bw.WriteByte('*'); err != nil {
		return err
	}
	if err := writeInt(bw, int64(len(args)-1)); err != nil {
		return err
	}
	if _, err := bw.WriteString(crlf); err != nil {
		return err
	}
	for _, a := range args[1:] {
		if err := writeBulk(bw, a); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// writeBulk 写一个 RESP Bulk String：`$<len>\r\n<body>\r\n`。
func writeBulk(bw *bufio.Writer, b []byte) error {
	if err := bw.WriteByte('$'); err != nil {
		return err
	}
	if err := writeInt(bw, int64(len(b))); err != nil {
		return err
	}
	if _, err := bw.WriteString(crlf); err != nil {
		return err
	}
	if _, err := bw.Write(b); err != nil {
		return err
	}
	_, err := bw.WriteString(crlf)
	return err
}

// ReadRequest 解析一条命令。行读取一律以 `\n` 结束并容忍 `\r\n`，
// 这样手工用 `nc` 输入（只会给 `\n`）也能被正确接受。
func (c *respCodec) ReadRequest(r io.Reader) ([][]byte, error) {
	br := c.br
	line, err := readLine(br)
	if err != nil {
		return nil, err
	}
	if line[0] != '$' {
		return nil, fmt.Errorf("resp: request must start with bulk command name, got %q", truncate(line))
	}
	name, err := readBulkBody(br, line[1:])
	if err != nil {
		return nil, err
	}
	line, err = readLine(br)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("resp: request missing arg count, got %q", truncate(line))
	}
	argc, err := parseInt(line[1:])
	if err != nil || argc < 0 {
		return nil, fmt.Errorf("resp: bad arg count %q", truncate(line))
	}
	if argc > MaxArgs {
		return nil, fmt.Errorf("resp: too many args: %d (limit %d)", argc, MaxArgs)
	}
	args := make([][]byte, 0, argc+1)
	args = append(args, name)
	for i := 0; i < argc; i++ {
		hdr, err := readLine(br)
		if err != nil {
			return nil, err
		}
		if len(hdr) == 0 || hdr[0] != '$' {
			return nil, fmt.Errorf("resp: expected bulk arg %d, got %q", i, truncate(hdr))
		}
		blk, err := readBulkBody(br, hdr[1:])
		if err != nil {
			return nil, err
		}
		args = append(args, blk)
	}
	return args, nil
}

// readBulkBody 读完一个 Bulk String 的体（长度已在 hdr 里给出）及其尾部 CRLF。
func readBulkBody(br *bufio.Reader, hdr []byte) ([]byte, error) {
	n, err := parseInt(hdr)
	if err != nil {
		return nil, fmt.Errorf("resp: bad bulk length %q", truncate(hdr))
	}
	if n < 0 || n > MaxBlobBytes {
		return nil, ErrTooLarge
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(br, body); err != nil {
		return nil, shortRead(err)
	}
	if _, err := readLine(br); err != nil { // 体后的 CRLF
		return nil, err
	}
	return body, nil
}

// ---- 应答 ----

// WriteResponse 写出一条应答。
//
// 形态：
//
//	:<status>\r\n            —— 状态类别，十进制，见 Status
//	+<statusname>\r\n        —— 与之等价的冗余可读行，便于 tcpdump/nc 直接看
//	<len>\r\n<body>\r\n      —— 每个负载块一条，长度前缀（RESP Bulk String 的体）
//	\r\n                     —— 报文结束
//
// 状态既给数字又给名字，是为了让"手敲请求 + 抓包看应答"这条调试路径可用：
// 数字机器好解析，名字人眼好读，两者同时出现不会有歧义。
func (c *respCodec) WriteResponse(w io.Writer, status Status, payload ...[]byte) error {
	bw := c.bw
	if err := bw.WriteByte(':'); err != nil {
		return err
	}
	if err := writeInt(bw, int64(status)); err != nil {
		return err
	}
	if _, err := bw.WriteString(crlf); err != nil {
		return err
	}
	if err := bw.WriteByte('+'); err != nil {
		return err
	}
	if _, err := bw.WriteString(status.String()); err != nil {
		return err
	}
	if _, err := bw.WriteString(crlf); err != nil {
		return err
	}
	for _, p := range payload {
		if err := writeInt(bw, int64(len(p))); err != nil {
			return err
		}
		if _, err := bw.WriteString(crlf); err != nil {
			return err
		}
		if _, err := bw.Write(p); err != nil {
			return err
		}
		if _, err := bw.WriteString(crlf); err != nil {
			return err
		}
	}
	if _, err := bw.WriteString(crlf); err != nil {
		return err
	}
	return bw.Flush()
}

// ReadResponse 解析一条应答。状态行必须自洽（数字与名字同类别），
// 否则按协议错误处理——这样实现分歧会在联调期立刻暴露，而不是被静默容忍。
func (c *respCodec) ReadResponse(r io.Reader) (Status, [][]byte, error) {
	br := c.br
	line, err := readLine(br)
	if err != nil {
		return 0, nil, err
	}
	if len(line) == 0 {
		return 0, nil, fmt.Errorf("resp: empty response")
	}
	if line[0] != ':' {
		return 0, nil, fmt.Errorf("resp: response must start with status, got %q", truncate(line))
	}
	n, err := parseInt(line[1:])
	if err != nil || n < 0 || n > 255 {
		return 0, nil, fmt.Errorf("resp: bad status %q", truncate(line))
	}
	st := Status(n)
	if !st.valid() {
		return 0, nil, fmt.Errorf("resp: unknown status %d", n)
	}
	name, err := readLine(br)
	if err != nil {
		return 0, nil, err
	}
	if len(name) == 0 || name[0] != '+' {
		return 0, nil, fmt.Errorf("resp: missing status name line, got %q", truncate(name))
	}
	if got := string(name[1:]); got != st.String() {
		return 0, nil, fmt.Errorf("resp: status mismatch: %d (%s) vs name %q", n, st, got)
	}
	var payload [][]byte
	for {
		line, err := readLine(br)
		if err != nil {
			return 0, nil, err
		}
		if len(line) == 0 {
			return st, payload, nil // 报文结束
		}
		ln, err := parseInt(line)
		if err != nil || ln < 0 {
			return 0, nil, fmt.Errorf("resp: bad block length %q", truncate(line))
		}
		if ln > MaxBlobBytes {
			return 0, nil, ErrTooLarge
		}
		body := make([]byte, ln)
		if _, err := io.ReadFull(br, body); err != nil {
			return 0, nil, shortRead(err)
		}
		if _, err := readLine(br); err != nil { // 体后的 CRLF
			return 0, nil, err
		}
		payload = append(payload, body)
	}
}

// ---- 底层读写 ----

// readLine 读一行并去掉行尾的 CRLF 或 LF。
func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			return nil, fmt.Errorf("resp: line too long (limit %d)", br.Size())
		}
		return nil, shortRead(err)
	}
	line = line[:len(line)-1] // 去掉 '\n'
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	return line, nil
}

// writeInt 以小端无关的十进制写整数（RESP 的整数同样是十进制文本）。
func writeInt(bw *bufio.Writer, v int64) error {
	var buf [20]byte
	return writeBytes(bw, strconv.AppendInt(buf[:0], v, 10))
}

func writeBytes(bw *bufio.Writer, b []byte) error {
	_, err := bw.Write(b)
	return err
}

func parseInt(b []byte) (int, error) {
	if len(b) == 0 || len(b) > 19 {
		return 0, fmt.Errorf("bad int %q", truncate(b))
	}
	n, err := strconv.Atoi(string(b))
	if err != nil {
		return 0, err
	}
	return n, nil
}

// shortRead 把"读到一半 EOF"统一成 ErrShortRead，便于上层识别为断连
// （与真正的解析错误区分开：前者可重试，后者说明两端实现不一致）。
func shortRead(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return ErrShortRead
	}
	return err
}

// truncate 限制错误消息里回显的原始字节，避免把超长脏数据整个写进日志。
func truncate(b []byte) string {
	const max = 32
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "..."
}
