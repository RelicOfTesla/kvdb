package codec

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// TextProto 是"类 SSDB"的文本协议 codec：帧格式沿用 SSDB 原生的
// `<十进制长度>\n<body>\n` 多重记录 + 空行结束，但**命令名与状态字都属 kvdb
// 自己**（SET/GET/QPUSH…、ok/not_found/unsupported…）。
//
// 它**不与 SSDB 互通**——只是借用了那套帧格式。这样做的理由：
//
//   - 文本长度前缀让人能用 nc/printf 手打请求来排查问题（与 RESP codec 同样的好处）；
//   - 记录之间不转义、长度显式，二进制安全，比 RESP 的行内参数更省事；
//   - 与 binary codec 相比多花一点字节（十进制长度 + 两个换行），换来可读性。
//
// 帧格式：
//
//	请求：rec[0]=命令名，rec[1..]=参数，每条记录 `<len>\n<body>\n`，末尾一个空行
//	应答：rec[0]=状态字，rec[1..]=负载，同上
//
// 状态字是本 SDK 自己的映射（不是 SSDB 的那套 client_error/fail 组合）：
//
//	ok            StatusOK
//	empty         StatusEmpty          查无此项（Get 未命中、队列为空）
//	error         StatusError
//	unsupported   StatusUnsupported    底座未实现该能力
//	closed        StatusClosed         底座已关闭
//	not_integer   StatusNotInteger     Incr 遇到非十进制整数
//	invalid_ttl   StatusInvalidTTL     TTL<=0
//	not_found     StatusNotFound       以错误形态上报的"不存在"
//	auth_required StatusAuthRequired   认证前被拒
//	auth_failed   StatusAuthFailed     认证失败
//
// 读取侧对未知状态字直接报错，不会静默当成成功——协议不匹配必须立刻暴露。
type textProtoCodec struct {
	br *bufio.Reader
	bw *bufio.Writer
}

// NewTextProto 返回类 SSDB 的文本协议编解码器。
func NewTextProto(r io.Reader, w io.Writer) Codec {
	return &textProtoCodec{
		br: bufio.NewReaderSize(r, 32*1024),
		bw: bufio.NewWriterSize(w, 32*1024),
	}
}

func (*textProtoCodec) Name() string { return "textproto" }

// WriteRequest 写出一条命令（记录序列 + 结束空行），并 Flush。
func (c *textProtoCodec) WriteRequest(w io.Writer, args [][]byte) error {
	if len(args) == 0 {
		return fmt.Errorf("textproto: empty command")
	}
	if len(args) > MaxArgs {
		return fmt.Errorf("textproto: too many args: %d (limit %d)", len(args), MaxArgs)
	}
	for _, a := range args {
		if len(a) > MaxBlobBytes {
			return fmt.Errorf("%w: %d bytes", ErrTooLarge, len(a))
		}
	}
	if err := c.writeRecs(args); err != nil {
		return err
	}
	return c.bw.Flush()
}

// ReadRequest 读入一条命令。
func (c *textProtoCodec) ReadRequest(r io.Reader) ([][]byte, error) {
	recs, err := c.readRecs()
	if err != nil {
		return nil, err
	}
	if len(recs) == 0 {
		return nil, fmt.Errorf("textproto: empty request")
	}
	if len(recs) > MaxArgs {
		return nil, fmt.Errorf("textproto: too many args: %d (limit %d)", len(recs), MaxArgs)
	}
	return recs, nil
}

// WriteResponse 写出一条应答（状态字 + 负载），并 Flush。
func (c *textProtoCodec) WriteResponse(w io.Writer, status Status, payload ...[]byte) error {
	name, err := textProtoStatusName(status)
	if err != nil {
		return err
	}
	recs := make([][]byte, 0, len(payload)+1)
	recs = append(recs, []byte(name))
	recs = append(recs, payload...)
	if err := c.writeRecs(recs); err != nil {
		return err
	}
	return c.bw.Flush()
}

// ReadResponse 读入一条应答。
func (c *textProtoCodec) ReadResponse(r io.Reader) (Status, [][]byte, error) {
	recs, err := c.readRecs()
	if err != nil {
		return 0, nil, err
	}
	if len(recs) == 0 {
		return 0, nil, fmt.Errorf("textproto: empty response")
	}
	st, err := textProtoStatusFromName(string(recs[0]))
	if err != nil {
		return 0, nil, err
	}
	return st, recs[1:], nil
}

// writeRecs 按 `<len>\n<body>\n` 写多条记录，末尾补空行作为报文结束标记。
func (c *textProtoCodec) writeRecs(recs [][]byte) error {
	for _, a := range recs {
		if _, err := c.bw.WriteString(strconv.Itoa(len(a))); err != nil {
			return err
		}
		if err := c.bw.WriteByte('\n'); err != nil {
			return err
		}
		if _, err := c.bw.Write(a); err != nil {
			return err
		}
		if err := c.bw.WriteByte('\n'); err != nil {
			return err
		}
	}
	return c.bw.WriteByte('\n') // 报文结束空行
}

// readRecs 读到空行为止，返回本次报文的全部记录。
func (c *textProtoCodec) readRecs() ([][]byte, error) {
	var recs [][]byte
	for {
		line, err := c.br.ReadString('\n')
		if err != nil {
			if err == io.EOF && line == "" {
				// 干净的报文边界处断连：对端正常关闭。
				return nil, fmt.Errorf("%w: connection closed by peer", ErrShortRead)
			}
			// 长度行读了一半就断：同样是截断。
			return nil, shortReadIfEOF(err)
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if line == "" {
			return recs, nil // 空行 = 报文结束
		}
		n, err := strconv.Atoi(line)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("textproto: bad size line %q", line)
		}
		if n > MaxBlobBytes {
			return nil, fmt.Errorf("%w: declared %d bytes (limit %d)", ErrTooLarge, n, MaxBlobBytes)
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(c.br, body); err != nil {
			return nil, shortReadIfEOF(err)
		}
		// 每条记录后的换行尾标
		if _, err := c.br.ReadString('\n'); err != nil {
			return nil, shortReadIfEOF(err)
		}
		recs = append(recs, body)
	}
}

// shortReadIfEOF 把"读到一半就没数据了"归一成 ErrShortRead。
// 裸 EOF 会让调用方无法区分"对端正常关闭"与"报文被截断"——前者可重连，
// 后者说明流已不可信。io.ReadFull 在部分读时返回 ErrUnexpectedEOF、
// 一个字节都没读到才返回 EOF，两者都属于截断。
func shortReadIfEOF(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return fmt.Errorf("%w: truncated frame", ErrShortRead)
	}
	return err
}

// textProtoStatusName 把抽象 status 映射为协议状态字。
func textProtoStatusName(s Status) (string, error) {
	switch s {
	case StatusOK:
		return "ok", nil
	case StatusEmpty:
		return "empty", nil
	case StatusError:
		return "error", nil
	case StatusUnsupported:
		return "unsupported", nil
	case StatusClosed:
		return "closed", nil
	case StatusNotInteger:
		return "not_integer", nil
	case StatusInvalidTTL:
		return "invalid_ttl", nil
	case StatusNotFound:
		return "not_found", nil
	case StatusAuthRequired:
		return "auth_required", nil
	case StatusAuthFailed:
		return "auth_failed", nil
	}
	return "", fmt.Errorf("textproto: unknown status %d", uint8(s))
}

// textProtoStatusFromName 是 textProtoStatusName 的逆映射。
// 大小写不敏感：本 SDK 一律小写，但对端实现可能大小写不一。
func textProtoStatusFromName(name string) (Status, error) {
	switch strings.ToLower(name) {
	case "ok":
		return StatusOK, nil
	case "empty":
		return StatusEmpty, nil
	case "error":
		return StatusError, nil
	case "unsupported":
		return StatusUnsupported, nil
	case "closed":
		return StatusClosed, nil
	case "not_integer":
		return StatusNotInteger, nil
	case "invalid_ttl":
		return StatusInvalidTTL, nil
	case "not_found":
		return StatusNotFound, nil
	case "auth_required":
		return StatusAuthRequired, nil
	case "auth_failed":
		return StatusAuthFailed, nil
	}
	return 0, fmt.Errorf("textproto: unknown status word %q", name)
}
