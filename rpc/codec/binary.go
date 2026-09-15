package codec

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// Binary 是长度前缀二进制帧 codec，作为 RESP 之外的第二套内置实现。
//
// 它存在的意义有两个：证明 Codec 抽象不只为 RESP 一家而设；以及在
// "不想让参数走文本转义"的场合提供一个确定性更强的选择——所有整数都是
// uvarint，所有块都是 `<uvarint 长度><原始字节>`，没有任何转义或文本往返。
//
// 帧格式：
//
//	请求：uvarint(argc)  然后每参数 <uvarint len><bytes>
//	应答：byte(status)   uvarint(count)  然后每块 <uvarint len><bytes>
//
// 与 RESP 相比：没有 CRLF、没有转义、整数二进制化；代价是不能直接手测。
type binCodec struct {
	br *bufio.Reader
	bw *bufio.Writer
}

// NewBinary 返回二进制帧编解码器。
func NewBinary(r io.Reader, w io.Writer) Codec {
	return &binCodec{
		br: bufio.NewReaderSize(r, 32*1024),
		bw: bufio.NewWriterSize(w, 32*1024),
	}
}

func (*binCodec) Name() string { return "binary" }

func (c *binCodec) WriteRequest(w io.Writer, args [][]byte) error {
	if len(args) == 0 {
		return fmt.Errorf("binary: empty command")
	}
	if len(args) > MaxArgs {
		return fmt.Errorf("binary: too many args: %d", len(args))
	}
	bw := c.bw
	if err := writeUvarint(bw, uint64(len(args))); err != nil {
		return err
	}
	for _, a := range args {
		if err := writeUvarint(bw, uint64(len(a))); err != nil {
			return err
		}
		if _, err := bw.Write(a); err != nil {
			return err
		}
	}
	return bw.Flush()
}

func (c *binCodec) ReadRequest(r io.Reader) ([][]byte, error) {
	br := c.br
	argc, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, shortRead(err)
	}
	if argc == 0 || argc > MaxArgs {
		return nil, fmt.Errorf("binary: bad argc %d", argc)
	}
	args := make([][]byte, 0, argc)
	for i := uint64(0); i < argc; i++ {
		b, err := readBlob(br)
		if err != nil {
			return nil, err
		}
		args = append(args, b)
	}
	return args, nil
}

func (c *binCodec) WriteResponse(w io.Writer, status Status, payload ...[]byte) error {
	bw := c.bw
	if err := bw.WriteByte(byte(status)); err != nil {
		return err
	}
	if err := writeUvarint(bw, uint64(len(payload))); err != nil {
		return err
	}
	for _, p := range payload {
		if err := writeUvarint(bw, uint64(len(p))); err != nil {
			return err
		}
		if _, err := bw.Write(p); err != nil {
			return err
		}
	}
	return bw.Flush()
}

func (c *binCodec) ReadResponse(r io.Reader) (Status, [][]byte, error) {
	br := c.br
	b, err := br.ReadByte()
	if err != nil {
		return 0, nil, shortRead(err)
	}
	st := Status(b)
	if !st.valid() {
		return 0, nil, fmt.Errorf("binary: unknown status %d", b)
	}
	n, err := binary.ReadUvarint(br)
	if err != nil {
		return 0, nil, shortRead(err)
	}
	if n > MaxArgs {
		return 0, nil, fmt.Errorf("binary: bad block count %d", n)
	}
	var payload [][]byte
	for i := uint64(0); i < n; i++ {
		blk, err := readBlob(br)
		if err != nil {
			return 0, nil, err
		}
		payload = append(payload, blk)
	}
	return st, payload, nil
}

func readBlob(br *bufio.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, shortRead(err)
	}
	if n > MaxBlobBytes {
		return nil, ErrTooLarge
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(br, b); err != nil {
		return nil, shortRead(err)
	}
	return b, nil
}

func writeUvarint(bw *bufio.Writer, v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, err := bw.Write(buf[:n])
	return err
}
