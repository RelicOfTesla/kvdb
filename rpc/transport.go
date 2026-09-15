package rpc

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/RelicOfTesla/kvdb/rpc/codec"
)

// ErrAuthFailed 是 c/s 认证失败的哨兵：口令错、挑战应答不匹配、
// 或未认证就发数据命令（服务端返回 auth_required 时同样归到这里）。
var ErrAuthFailed = errors.New("rpc: authentication failed")

// ErrProtocol 表示两端报文不通：格式不符、codec 不匹配、或对端在报文中途断开。
// 它与"网络暂时不可用"区分开——后者可以重试，前者重试多少次都一样。
var ErrProtocol = errors.New("rpc: protocol error")

// transport 是一条 RPC 连接上的收发原语：codec + 读写串行化。
//
// 它**不是**并发安全的调用入口：call 内部用 mu 把"写请求-读应答"这一对
// 操作串起来（协议上必须一一对应），因此同一条连接上的并发调用会被排队，
// 而不会把响应错配给别的调用者。真正的并发靠连接池（见 client.go）。
type transport struct {
	c   net.Conn
	cdc codec.Codec
	mu  sync.Mutex
	// deadline 记录本连接当前生效的截止时间（零值表示未设）。
	// 必须自己记：net.Conn 接口只有 SetDeadline，**没有**读回 deadline 的
	// 方法，靠类型断言去拿并不通用（且会让"尊重调用方更短的超时"静默失效）。
	deadline time.Time
}

func newTransport(c net.Conn, cdc codec.Codec) *transport {
	return &transport{c: c, cdc: cdc}
}

// call 发送一条命令并读取应答。返回值语义与 codec.ReadResponse 一致。
//
// 调用前应已通过 setDeadline 设好本次往返的截止时间。**读到应答之前绝不
// 无限等待**：两端 codec 不一致时，双方会各自停在 Read 上互等，
// 表现为整个程序挂死——一个配置错误不该有这种后果。因此这里兜一层
// "读到一半卡住"的保护（见 readGuard）。
func (t *transport) call(args ...[]byte) (codec.Status, [][]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.cdc.WriteRequest(t.c, args); err != nil {
		return 0, nil, connError(err)
	}
	if err := t.armReadGuard(); err != nil {
		return 0, nil, connError(err)
	}
	st, payload, err := t.cdc.ReadResponse(t.c)
	if err != nil {
		return 0, nil, connError(err)
	}
	return st, payload, nil
}

// readGuard 是"本次往返必须有截止时间"的兜底。
//
// 正常情况下调用方会按 ctx 设好 deadline；但 Open 阶段的能力探测、
// 以及调用方给了无 deadline 的 ctx 时，连接上就没有任何超时。
// 此时若对端在等我们（或两端格式不通），就会永久阻塞。
// 兜底值取得足够宽松（不误伤长命令），但保证错误配置最终以错误收场。
var readGuard = 30 * time.Second

// armReadGuard 保证本次往返一定有截止时间——但**不覆盖**调用方设置的
// 更短截止时间（握手/认证依赖它把 codec 配错变成快速失败）。
//
// 调用方须已持有 t.mu（由 call 调用），因此这里不再加锁。
func (t *transport) armReadGuard() error {
	if cur := t.deadline; !cur.IsZero() && time.Until(cur) < readGuard {
		return nil // 已有更严格的截止时间，尊重它
	}
	guard := time.Now().Add(readGuard)
	t.deadline = guard
	return t.c.SetDeadline(guard)
}

// setDeadline 设置（或清除）本连接的截止时间，并记住它。
func (t *transport) setDeadline(dl time.Time) {
	t.mu.Lock()
	t.deadline = dl
	t.mu.Unlock()
	_ = t.c.SetDeadline(dl)
}

// Close 关闭底层连接。幂等。
func (t *transport) Close() error { return t.c.Close() }

// connError 把底层 IO 错误归一成哨兵，便于上层判定"这条连接废了"。
func connError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, codec.ErrShortRead) || errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return err
	}
	return err
}

// authError 把服务端返回的认证失败状态转成哨兵错误。
func authError(st codec.Status, payload [][]byte) error {
	msg := firstString(payload)
	if msg == "" {
		msg = st.String()
	}
	return fmt.Errorf("%w: %s", ErrAuthFailed, msg)
}

// firstString 取第一个负载块作为文本（无则空串）。
func firstString(payload [][]byte) string {
	if len(payload) == 0 {
		return ""
	}
	return string(payload[0])
}
