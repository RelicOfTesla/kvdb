package rpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/rpc/codec"
)

// ServerConfig 是 RPC 服务端配置。零值不可用——至少需要 Backend。
type ServerConfig struct {
	// Network / Addr 是监听地址，语义同 net.Listen（"tcp" + ":7788"）。
	// 留空按 tcp + 127.0.0.1:0（随机端口，主要给测试用）。
	Network string
	Addr    string

	// Backend 是**服务器侧的基座 URI**，如 "jsonl://./data.jsonl"、
	// "bolt://./data.bolt"、"mem://"。它决定数据实际存在哪里，client 对此
	// 完全无感——这正是 c/s 隔离的意义：底座可以随时替换而无需改客户端。
	//
	// 该 URI 由本进程已注册的 scheme 解析，因此 server 侧必须 import 对应基座包
	// （或 _ "github.com/RelicOfTesla/kvdb/all"）。
	Backend string

	// Opener 可替代 Backend：直接给定一个已构造好的基座（便于嵌入测试替身、
	// 或在宿主里复用同一个基座实例）。二者都提供时 Opener 优先。
	Opener func(ctx context.Context) (core.KvProvider, error)

	// Auth / Password 是 c/s 认证的开关与口令（见 AuthMode）。
	// 缺省 AuthNone；Password 在 AuthPlain/AuthChallenge 下必填，否则 NewServer 报错。
	Auth     AuthMode
	Password string

	// TLSConfig 非空则监听 TLS。**同一端口按配置切换**：给证书即 TLS，
	// 不给即明文，协议本身不区分（因此不需要 ALPN 或双端口）。
	TLSConfig *tls.Config

	// Codec 指定报文编解码实现，nil 取默认的 RESP（见 codec 包）。
	// 自定义实现时两端必须装配同一个。
	Codec codecFactory

	// MaxConns 限制同时建立的连接数（<=0 不限）。超出时新连接被立即关闭——
	// 不做排队：RPC 层排队会掩盖客户端的连接池配置错误。
	MaxConns int

	// MaxBatchOps 限制单次 BATCH 的操作条数（<=0 取 DefaultMaxBatchOps）。
	// 请求里声明的条数超限即拒绝，避免一个坏请求让服务端分配巨量内存。
	MaxBatchOps int

	// ConnTimeout 是单条命令的执行超时（<=0 表示不额外限制，由 ctx/连接超时决定）。
	// 到点后服务端放弃该连接上的后续处理并关闭连接。
	ConnTimeout time.Duration
}

// DefaultMaxBatchOps 是单次批写的默认上限。
const DefaultMaxBatchOps = 1 << 16

// Server 是一个 RPC 服务端：把本地基座暴露给远程 client。
type Server struct {
	cfg    ServerConfig
	ln     net.Listener
	codec  codecFactory
	prov   core.KvProvider
	caps   core.Caps
	closed atomic.Bool

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

// NewServer 装配服务端：打开 Backend、建监听。返回后即可 Serve。
func NewServer(ctx context.Context, cfg ServerConfig) (*Server, error) {
	if cfg.Opener == nil && cfg.Backend == "" {
		return nil, fmt.Errorf("rpc: ServerConfig needs Backend or Opener")
	}
	if cfg.Auth != AuthNone && cfg.Password == "" {
		return nil, fmt.Errorf("rpc: auth mode %s requires a password", cfg.Auth)
	}
	factory := cfg.Codec
	if factory == nil {
		factory = defaultCodecFactory
	}
	prov, err := openBackend(ctx, cfg)
	if err != nil {
		return nil, err
	}
	network, addr := cfg.Network, cfg.Addr
	if network == "" {
		network = "tcp"
	}
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen(network, addr)
	if err != nil {
		closeProvider(prov)
		return nil, fmt.Errorf("rpc: listen %s %s: %w", network, addr, err)
	}
	if cfg.TLSConfig != nil {
		ln = tls.NewListener(ln, cfg.TLSConfig)
	}
	return &Server{
		cfg:   cfg,
		ln:    ln,
		codec: factory,
		prov:  prov,
		caps:  capabilitiesOf(prov),
		conns: make(map[net.Conn]struct{}),
	}, nil
}

// Addr 返回实际监听地址（配置端口为 0 时用于拿到真实端口）。
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Capabilities 返回**底层基座**的真实能力，用于在测试里对照 client 侧上报值。
func (s *Server) Capabilities() core.Caps { return s.caps }

// Serve 接受连接直到 ctx 取消或 Close 被调用。每条连接一个 goroutine。
func (s *Server) Serve(ctx context.Context) error { return s.serve(ctx, s.ln) }

// ServeWithListener 用外部提供的 listener 服务，而不是 NewServer 建的那个。
//
// 存在的意义是测试：包一层自定义 listener 就能观察/篡改线路上的原始字节
// （如断言挑战认证不泄漏口令），而不必为了可测性在实现里塞钩子。
// 该 listener 必须是已经建好的（TLS 由调用方自行决定是否包装）。
func (s *Server) ServeWithListener(ln net.Listener) error { return s.serve(context.Background(), ln) }

func (s *Server) serve(ctx context.Context, ln net.Listener) error {
	defer s.wg.Wait()
	go func() {
		<-ctx.Done()
		s.Close()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			if s.closed.Load() || errorsIsClosed(err) {
				return nil
			}
			return err
		}
		if !s.trackConn(c) {
			c.Close()
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrackConn(c)
			// 每条连接兜一层 recover：面向网络的进程里，单个连接的解析/处理
			// panic 绝不能打死整个服务端（一个畸形报文就能远程打崩它）。
			// 只隔离当前连接——该连接被关闭，客户端看到断连，服务继续可用。
			//
			// 这是**兜底**而不是许可证：codec 仍必须自行校验输入
			//（resp 曾因未判空行而 line[0] panic，已单独修复）。
			// 包内不引日志依赖：调用方要记录的话，从 Close/Err 侧自行观测。
			defer func() { _ = recover() }()
			s.serveConn(ctx, c)
		}()
	}
}

// Close 停止监听并关闭全部在途连接。基座本身也一并关闭（它的生命周期由
// server 拥有——client 的 Close 只关自己的连接，不会关掉服务端基座）。
func (s *Server) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	err := s.ln.Close()
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	closeProvider(s.prov)
	return err
}

func (s *Server) trackConn(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.MaxConns > 0 && len(s.conns) >= s.cfg.MaxConns {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrackConn(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
	c.Close()
}

// serveConn 处理一条连接：先做 c/s 认证，然后循环分发命令。
//
// 认证是**连接级**的：握手一次，后续命令复用；连接断开即失效（客户端重连
// 会重新握手）。挑战模式的 nonce 也只在这条连接的这次握手内有效，
// 因此不存在跨连接的挑战重放。
func (s *Server) serveConn(ctx context.Context, c net.Conn) {
	tr := newTransport(c, s.codec(c, c))
	authed := s.cfg.Auth == AuthNone
	var challenge []byte // 挑战模式的待验证 nonce

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if s.cfg.ConnTimeout > 0 {
			tr.setDeadline(time.Now().Add(s.cfg.ConnTimeout))
		}
		args, err := tr.cdc.ReadRequest(tr.c)
		if err != nil {
			return // 对端关闭或协议错误：结束这条连接
		}
		if len(args) == 0 {
			continue
		}
		cmd := string(args[0])

		// HELLO 是连接建立后的第一条命令，用于校验两端 codec 一致。
		if cmd == cmdHello {
			if !s.handleHello(tr, args[1:]) {
				return // codec 不匹配：连接已不可用，直接结束
			}
			continue
		}
		// 认证命令在鉴权闸门之前处理，且不需要已认证。
		if cmd == cmdAuthPlain || cmd == cmdAuthChallenge {
			authed, challenge = s.handleAuth(tr, cmd, args[1:], authed, challenge)
			continue
		}
		if !authed {
			_ = tr.cdc.WriteResponse(tr.c, codec.StatusAuthRequired, []byte("authentication required"))
			continue
		}
		// PING 是唯一不碰基座的命令：给"探活/验认证"用。
		if cmd == cmdPing {
			_ = tr.cdc.WriteResponse(tr.c, codec.StatusOK)
			continue
		}
		status, payload := s.dispatch(ctx, cmd, args[1:])
		if err := tr.cdc.WriteResponse(tr.c, status, payload...); err != nil {
			return
		}
	}
}
