package ssdb

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// conn 是 SSDB 原生文本协议的单连接客户端。协议为多条
// `<十进制长度>\n<body>\n` 记录序列，以空行 `\n` 结尾：
// 请求首条记录是命令名，响应首条记录是状态（ok/not_found/error/client_error/fail）。
// 与 SSDB 源码 net/link.cpp 的 Link::send/recv 编码一一对应。
type conn struct {
	c  net.Conn
	br *bufio.Reader
	bw *bufio.Writer
}

// maxRecordBytes 是单条记录（长度前缀声明的 body）的防御性字节上限，
// 防止服务端异常/恶意应答超大长度导致按声明值一次性分配内存。
const maxRecordBytes = 1 << 26 // 64 MiB

// dial 建立到 addr（host:port）的连接；ctx 控制拨号与后续每操作超时。
// 无 ctx deadline 时拨号默认 10s 超时，避免对黑洞地址阻塞到内核 SYN 超时。
func dial(ctx context.Context, addr string) (*conn, error) {
	d := net.Dialer{Timeout: 10 * time.Second}
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("ssdb: dial %s: %w", addr, err)
	}
	if tcp, ok := nc.(*net.TCPConn); ok {
		tcp.SetNoDelay(true)
	}
	return &conn{c: nc, br: bufio.NewReaderSize(nc, 32*1024), bw: bufio.NewWriterSize(nc, 32*1024)}, nil
}

// request 发送命令并读取完整响应。返回状态与负载记录；响应结束由空行判定。
func (c *conn) request(ctx context.Context, args ...[]byte) (status string, payload [][]byte, err error) {
	c.setDeadline(ctx)
	if err := c.writeReq(args); err != nil {
		return "", nil, err
	}
	if err := c.bw.Flush(); err != nil {
		return "", nil, err
	}
	return c.readOne()
}

// resp 是一条命令的响应。
type resp struct {
	status  string
	payload [][]byte
}

// pipeline 以流水线方式发送多条命令：先全部写入并只 Flush 一次，再按序读取
// 全部响应。SSDB 无事务，流水线只降低往返次数，不提供整批原子性。
func (c *conn) pipeline(ctx context.Context, reqs [][][]byte) ([]resp, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	c.setDeadline(ctx)
	for _, args := range reqs {
		if err := c.writeReq(args); err != nil {
			return nil, err
		}
	}
	if err := c.bw.Flush(); err != nil {
		return nil, err
	}
	out := make([]resp, len(reqs))
	for i := range reqs {
		st, payload, err := c.readOne()
		if err != nil {
			return nil, err
		}
		out[i] = resp{status: st, payload: payload}
	}
	return out, nil
}

func (c *conn) setDeadline(ctx context.Context) {
	if dl, ok := ctx.Deadline(); ok {
		c.c.SetDeadline(dl)
	} else {
		c.c.SetDeadline(time.Time{})
	}
}

// writeReq 按帧写入一条命令（不 Flush）。
func (c *conn) writeReq(args [][]byte) error {
	for _, a := range args {
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

// readOne 读取一条完整响应（状态 + 负载）。
func (c *conn) readOne() (string, [][]byte, error) {
	recs, err := c.readRecs()
	if err != nil {
		return "", nil, err
	}
	if len(recs) == 0 {
		return "", nil, fmt.Errorf("ssdb: empty response")
	}
	return string(recs[0]), recs[1:], nil
}

// readRecs 读取一条完整报文（多条记录，空行结束）。
func (c *conn) readRecs() ([][]byte, error) {
	var recs [][]byte
	for {
		line, err := c.br.ReadString('\n')
		if err != nil {
			if err == io.EOF && line == "" {
				return nil, fmt.Errorf("ssdb: connection closed by server")
			}
			return nil, err
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if line == "" {
			return recs, nil // 空行 = 报文结束
		}
		n, err := strconv.Atoi(line)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("ssdb: bad size line %q", line)
		}
		if n > maxRecordBytes {
			return nil, fmt.Errorf("ssdb: record too large: %d bytes (limit %d)", n, maxRecordBytes)
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(c.br, body); err != nil {
			return nil, err
		}
		// 每条记录后的换行尾标
		if _, err := c.br.ReadString('\n'); err != nil {
			return nil, err
		}
		recs = append(recs, body)
	}
}
