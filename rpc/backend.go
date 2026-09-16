package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/rpc/codec"
)

// ---- codec 装配 ----

// codecFactory 按一条连接构造编解码器。两端各持一个工厂，
// 而不是共享一个 Codec 实例——Codec 带缓冲，必须一条连接一个。
type codecFactory func(r io.Reader, w io.Writer) codec.Codec

// CodecRESP / CodecBinary / CodecTextProto 是内置工厂，供 ServerConfig.Codec /
// Config.Codec 直接引用。
func CodecRESP(r io.Reader, w io.Writer) codec.Codec      { return codec.NewRESP(r, w) }
func CodecBinary(r io.Reader, w io.Writer) codec.Codec    { return codec.NewBinary(r, w) }
func CodecTextProto(r io.Reader, w io.Writer) codec.Codec { return codec.NewTextProto(r, w) }

// defaultCodecFactory 是缺省实现：RESP（仿 Redis 协议）。
func defaultCodecFactory(r io.Reader, w io.Writer) codec.Codec { return codec.NewRESP(r, w) }

// ---- 基座装配 ----

// openBackend 按配置打开服务端基座。
func openBackend(ctx context.Context, cfg ServerConfig) (core.KvProvider, error) {
	if cfg.Opener != nil {
		p, err := cfg.Opener(ctx)
		if err != nil {
			return nil, fmt.Errorf("rpc: open backend: %w", err)
		}
		if p == nil {
			return nil, fmt.Errorf("rpc: Opener returned nil provider")
		}
		return p, nil
	}
	db, err := kvdb.Open(ctx, cfg.Backend)
	if err != nil {
		return nil, fmt.Errorf("rpc: open backend %s: %w", redactBackend(cfg.Backend), err)
	}
	// kvdb.DB 是"KV + 全部可选能力 + Close"的完整门面；这里要的是它背后的
	// **原始基座**，因为可选能力的断言必须落在基座上——断言 kvdb.DB 恒为真，
	// 断言原始基座才能知道它究竟实现了什么。
	if u, ok := db.(interface{ KvProvider() core.KvProvider }); ok {
		return u.KvProvider(), nil
	}
	return db, nil
}

// capabilitiesOf 读取基座能力。RPC 层**如实透传**：client 的
// Capabilities() 必须与"本地直连同一基座"得到的结果一致，
// 包括 BatchComposed —— RPC 不替底层基座许诺它没有的性质。
func capabilitiesOf(p core.KvProvider) core.Caps {
	c := core.Caps{}
	if _, ok := p.(core.QueueProvider); ok {
		c.Queue = true
	}
	if _, ok := p.(core.ZSetProvider); ok {
		c.ZSet = true
	}
	if _, ok := p.(core.BatchProvider); ok {
		c.Batch = true
	}
	if _, ok := p.(core.BatchComposedProvider); ok {
		c.BatchComposed = true
	}
	if w, ok := p.(core.IncrWrapsProvider); ok {
		c.IncrWraps = w.IncrWraps()
	}
	return c
}

func closeProvider(p core.KvProvider) {
	if p == nil {
		return
	}
	if c, ok := p.(core.Closer); ok {
		_ = c.Close()
	}
}

// redactBackend 脱敏：基座 URI 可能带凭据（mysql://user:pass@…），
// 而错误常被直接写日志。
func redactBackend(uri string) string {
	if i := indexOf(uri, "://"); i > 0 {
		return uri[:i] + "://<redacted>"
	}
	return "<redacted backend>"
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// errorsIsClosed 判定 Accept 的错误是否来自监听器被关闭（正常退出路径）。
func errorsIsClosed(err error) bool {
	return errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrClosed)
}

// ---- codec 握手 ----

// clientHello 声明本端使用的 codec 名。
func clientHello(tr *transport, name string) error {
	st, payload, err := tr.call([]byte(cmdHello), []byte(name))
	if err != nil {
		return err
	}
	if st != codec.StatusOK {
		return fmt.Errorf("%w: %s", ErrProtocol, firstString(payload))
	}
	return nil
}

// handleHello 校验对端 codec 名是否与本端一致。返回 false 表示握手失败、
// 连接必须关闭（此时已经给对端发过明确原因）。
func (s *Server) handleHello(tr *transport, args [][]byte) bool {
	if len(args) != 1 {
		_ = tr.cdc.WriteResponse(tr.c, codec.StatusError, []byte("rpc: HELLO expects 1 arg (codec name)"))
		return false
	}
	want := codecNameOf(s.codec)
	if got := string(args[0]); got != want {
		msg := fmt.Sprintf("rpc: codec mismatch: client sent %q, server uses %q", got, want)
		_ = tr.cdc.WriteResponse(tr.c, codec.StatusError, []byte(msg))
		return false
	}
	_ = tr.cdc.WriteResponse(tr.c, codec.StatusOK)
	return true
}

// emptyReader 是取 codec 名字时用的空 Reader（构造函数不读它）。
type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

// codecNameOf 返回工厂产出的实现名。用空 Reader 构造一个实例只为取名，
// 不产生任何 IO（codec 的构造函数不做 IO）。
func codecNameOf(f codecFactory) string {
	if f == nil {
		f = defaultCodecFactory
	}
	return f(emptyReader{}, io.Discard).Name()
}

// ---- 错误映射 ----

// statusFor 把基座返回的错误映射成协议状态。**这是"远程与本地行为一致"
// 的关键一环**：哨兵错误必须原样过线，client 侧再用 errors.Is 还原，
// 否则业务代码里 `errors.Is(err, core.ErrUnsupported)` 这类判断会在
// 换成 RPC 基座后静默失效。
func statusFor(err error) (codec.Status, [][]byte) {
	if err == nil {
		return codec.StatusOK, nil
	}
	st := codec.StatusError
	switch {
	case errors.Is(err, core.ErrUnsupported):
		st = codec.StatusUnsupported
	case errors.Is(err, core.ErrClosed):
		st = codec.StatusClosed
	case errors.Is(err, core.ErrNotInteger):
		st = codec.StatusNotInteger
	case errors.Is(err, core.ErrInvalidTTL):
		st = codec.StatusInvalidTTL
	case errors.Is(err, core.ErrNotFound):
		st = codec.StatusNotFound
	}
	return st, [][]byte{[]byte(err.Error())}
}

// errorForStatus 是 statusFor 的逆映射，供客户端还原哨兵错误。
func errorForStatus(st codec.Status, payload [][]byte) error {
	msg := firstString(payload)
	switch st {
	case codec.StatusUnsupported:
		return wrapSentinel(core.ErrUnsupported, msg)
	case codec.StatusClosed:
		return wrapSentinel(core.ErrClosed, msg)
	case codec.StatusNotInteger:
		return wrapSentinel(core.ErrNotInteger, msg)
	case codec.StatusInvalidTTL:
		return wrapSentinel(core.ErrInvalidTTL, msg)
	case codec.StatusNotFound:
		return wrapSentinel(core.ErrNotFound, msg)
	case codec.StatusError:
		if msg == "" {
			msg = "server error"
		}
		return errors.New(msg)
	}
	if msg == "" {
		msg = st.String()
	}
	return fmt.Errorf("rpc: unexpected status %s: %s", st, msg)
}

// wrapSentinel 保留哨兵身份（errors.Is 可判等）同时带上服务端原文。
//
// 服务端往往把哨兵**原样**返回（err.Error() 就等于 sentinel.Error()），
// 此时再包一层会得到 "kvdb: ttl must be positive: kvdb: ttl must be positive"
// 这种重复消息，因此内容相同时直接返回哨兵本身。
func wrapSentinel(sentinel error, msg string) error {
	if msg == "" || msg == sentinel.Error() {
		return sentinel
	}
	return fmt.Errorf("%w: %s", sentinel, msg)
}
