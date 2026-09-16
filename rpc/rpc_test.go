package rpc_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/kvdbtest"
	"github.com/RelicOfTesla/kvdb/mem"
	"github.com/RelicOfTesla/kvdb/rpc"
)

// 本文件的核心是 TestContract*：把**同一份** kvdbtest 合同用例分别跑在
// "本地直连 mem" 与 "经 RPC 连 mem" 两条路径上。它们全绿，才说明 RPC 客户端
// 在语义上真的是透明通道，而不是"长得像"的实现。

// startServer 起一个测试用 server，返回地址与清理函数。
func startServer(t *testing.T, cfg rpc.ServerConfig) (*rpc.Server, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	srv, err := rpc.NewServer(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatalf("NewServer: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = srv.Close()
		<-done
	})
	return srv, srv.Addr()
}

// memBackend 让 server 用内存基座（最快，且不落盘）。
func memBackend(t *testing.T) func(context.Context) (core.KvProvider, error) {
	t.Helper()
	return func(ctx context.Context) (core.KvProvider, error) {
		return mem.New(), nil
	}
}

// TestContractOverRPC 把合同用例跑在 RPC 通道上。
//
// 每条子用例各起一个 server（因此各有一份独立的 mem 基座）：kvdbtest 的
// newDB 会为每条子用例调用一次 factory，语义就是"全新的一只库"。若把
// startServer 提到外面只起一次，所有子用例就会共用同一份服务端数据——
// 那些"空库前提下"的断言（如 TestScanBoundaries 的 Scan("","") 应为空）
// 会看到前面子用例写入的残留而失败。本地直连的对照用例每条子用例都新建
// 基座，这里必须对齐同一隔离语义，否则 RPC 通道测的就不是同一件事。
func TestContractOverRPC(t *testing.T) {
	kvdbtest.RunWithOptions(t, kvdbtest.VirtualClock(t), func(t *testing.T) core.KvProvider {
		_, addr := startServer(t, rpc.ServerConfig{Opener: memBackend(t)})
		p, err := rpc.Open(t.Context(), addr)
		if err != nil {
			t.Fatalf("rpc.Open: %v", err)
		}
		t.Cleanup(func() { _ = p.Close() })
		return p
	})
}

// TestContractLocalBaseline 是上面那条用例的对照：同样的合同直接跑在 mem 上。
// 两条都通过，才能把差异归因到 RPC 层而不是合同本身。
func TestContractLocalBaseline(t *testing.T) {
	kvdbtest.RunWithOptions(t, kvdbtest.VirtualClock(t), func(t *testing.T) core.KvProvider {
		p := mem.New()
		t.Cleanup(func() { _ = p.Close() })
		return p
	})
}

// TestCapabilitiesForwarded 验证 client 如实上报**底层基座**的能力。
// 这是"client 不感知底座"的反面：它不需要知道底座是什么，
// 但必须如实转达底座能做什么。
func TestCapabilitiesForwarded(t *testing.T) {
	srv, addr := startServer(t, rpc.ServerConfig{Opener: memBackend(t)})
	p, err := rpc.Open(t.Context(), addr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	want := srv.Capabilities()
	got := p.Capabilities()
	if got != want {
		t.Fatalf("client 上报能力 %+v != server 底座 %+v", got, want)
	}
	if !got.Queue || !got.ZSet || !got.Batch {
		t.Fatalf("mem 底座应具备 queue/zset/batch，got %+v", got)
	}
	// mem 是同锁内逐条应用，批内可见；RPC 必须原样转达这一点。
	if !got.BatchComposed {
		t.Fatalf("mem 的 BatchComposed 应为 true，got %+v", got)
	}
}

// TestSentinelErrorsSurviveRPC 是这套协议里最容易出错的地方：
// 哨兵错误必须原样过线，否则业务代码里 errors.Is(err, core.ErrUnsupported)
// 这类判断会在换成 RPC 基座后静默失效。
func TestSentinelErrorsSurviveRPC(t *testing.T) {
	// kvOnly 是一个只实现 KV 的基座：用来触发 ErrUnsupported。
	_, addr := startServer(t, rpc.ServerConfig{Opener: func(ctx context.Context) (core.KvProvider, error) {
		return &kvOnly{}, nil
	}})
	p, err := rpc.Open(t.Context(), addr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	if caps := p.Capabilities(); caps.Queue || caps.ZSet || caps.Batch {
		t.Fatalf("kvOnly 不应报告 queue/zset/batch，got %+v", caps)
	}
	// 未实现的能力：client 侧必须能 errors.Is 出 ErrUnsupported。
	if err := p.QPush(ctx, "q", []byte("v")); !errors.Is(err, core.ErrUnsupported) {
		t.Fatalf("QPush 应 ErrUnsupported, got %v", err)
	}
	if _, _, err := p.QPop(ctx, "q"); !errors.Is(err, core.ErrUnsupported) {
		t.Fatalf("QPop 应 ErrUnsupported, got %v", err)
	}
	if err := p.ZSet(ctx, "z", "m", 1); !errors.Is(err, core.ErrUnsupported) {
		t.Fatalf("ZSet 应 ErrUnsupported, got %v", err)
	}
	if err := p.ApplyBatch(ctx, []core.BatchOp{{Kind: core.BatchSet, Key: "k", Value: []byte("v")}}); !errors.Is(err, core.ErrUnsupported) {
		t.Fatalf("ApplyBatch 应 ErrUnsupported, got %v", err)
	}

	// Incr 非整数 -> ErrNotInteger
	if err := p.Set(ctx, "s", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Incr(ctx, "s", 1); !errors.Is(err, core.ErrNotInteger) {
		t.Fatalf("Incr 非整数应 ErrNotInteger, got %v", err)
	}
	// 非正 TTL -> ErrInvalidTTL
	if err := p.SetEx(ctx, "k", []byte("v"), 0); !errors.Is(err, core.ErrInvalidTTL) {
		t.Fatalf("SetEx ttl=0 应 ErrInvalidTTL, got %v", err)
	}
	if err := p.Expire(ctx, "k", -1); !errors.Is(err, core.ErrInvalidTTL) {
		t.Fatalf("Expire ttl<0 应 ErrInvalidTTL, got %v", err)
	}
}

// TestRemoteBackendIsIsolated 验证"本地变远程"的实际价值：
// client 侧没有任何基座 import（本测试文件只 import 了 mem 供服务端使用），
// 通过 kvdb.Open("rpc://…") 就能拿到一个完整可用的 DB。
func TestRemoteBackendIsIsolated(t *testing.T) {
	srv, addr := startServer(t, rpc.ServerConfig{Opener: memBackend(t)})
	if !srv.Capabilities().Queue {
		t.Fatal("前置条件：底座应有 Queue 能力")
	}
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "rpc://"+addr)
	if err != nil {
		t.Fatalf("kvdb.Open(rpc://): %v", err)
	}
	defer db.Close()

	if err := db.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.QPush(ctx, "jobs", []byte("j1")); err != nil {
		t.Fatal(err)
	}
	if err := db.ZSet(ctx, "rank", "alice", 10); err != nil {
		t.Fatal(err)
	}
	if err := db.Batch(ctx, func(b *kvdb.Batch) error {
		b.Set("cnt", []byte("5"))
		b.ZIncr("rank", "alice", 2)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Incr(ctx, "cnt", 1); err != nil || v != 6 {
		t.Fatalf("批写后 Incr(cnt,1) = %d,%v want 6", v, err)
	}
	if sc, ok, _ := db.ZGet(ctx, "rank", "alice"); !ok || sc != 12 {
		t.Fatalf("批写后 ZGet(rank,alice) = %d,%v want 12", sc, ok)
	}
	if v, ok, _ := db.Get(ctx, "k"); !ok || string(v) != "v" {
		t.Fatalf("Get = %q,%v", v, ok)
	}
}

// TestCloseDoesNotCloseServer 验证生命周期边界：
// client 的 Close 只关自己的连接，服务端基座仍可被其他 client 使用。
func TestCloseDoesNotCloseServer(t *testing.T) {
	_, addr := startServer(t, rpc.ServerConfig{Opener: memBackend(t)})
	ctx := context.Background()

	c1, err := rpc.Open(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	if err := c1.Set(ctx, "shared", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// c1 关闭后自身应报 ErrClosed
	if err := c1.Set(ctx, "x", []byte("v")); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("关闭后应 ErrClosed, got %v", err)
	}

	// 另一个 client 仍能读到 c1 写入的数据：服务端基座没有被关掉。
	c2, err := rpc.Open(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if v, ok, err := c2.Get(ctx, "shared"); err != nil || !ok || string(v) != "v" {
		t.Fatalf("新 client Get = %q,%v,%v; 服务端基座不应被前一个 client 关闭", v, ok, err)
	}
}

// TestConcurrentClients 验证连接池：多条连接并发跑，结果不串味。
func TestConcurrentClients(t *testing.T) {
	_, addr := startServer(t, rpc.ServerConfig{Opener: memBackend(t)})
	p, err := rpc.OpenWithConfig(t.Context(), rpc.Config{Addr: addr, PoolSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	const workers, iters = 16, 40
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := fmt.Sprintf("w:%d", w)
			for i := 0; i < iters; i++ {
				if err := p.Set(ctx, key, []byte(key)); err != nil {
					errCh <- fmt.Errorf("Set: %w", err)
					return
				}
				// 读回必须是自己写的那把 key 的值：连接复用若串了请求，
				// 这里会读到别人的值（错配）。
				v, ok, err := p.Get(ctx, key)
				if err != nil {
					errCh <- fmt.Errorf("Get: %w", err)
					return
				}
				if !ok || string(v) != key {
					errCh <- fmt.Errorf("Get(%s) = %q,%v", key, v, ok)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

// TestProtocolErrorIsRetried 验证连接被服务端/中间设备掐断后，
// 客户端能用一条新连接自动重试一次，而不是把瞬时故障直接抛给业务。
func TestProtocolErrorIsRetried(t *testing.T) {
	srv, addr := startServer(t, rpc.ServerConfig{Opener: memBackend(t)})
	p, err := rpc.Open(t.Context(), addr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	if err := p.Set(ctx, "k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	// 关掉 server 会让在途/空闲连接失效。
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	// 这一步会失败（服务端已停），但必须是连接类错误而不是协议错乱。
	if _, _, err := p.Get(ctx, "k"); err == nil {
		t.Fatal("server 已关闭，Get 不应成功")
	}
}

// TestServerRestartRecovers 验证服务端重启（同地址）后客户端能自行恢复，
// 不需要重建 Provider —— 前提是它把旧连接判废、并拨一条新的。
func TestServerRestartRecovers(t *testing.T) {
	ctx := context.Background()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	// 先起一个 server 写入数据，然后停掉。
	srv1, err := rpc.NewServer(ctx, rpc.ServerConfig{Addr: addr, Opener: memBackend(t)})
	if err != nil {
		t.Fatal(err)
	}
	serveCtx1, cancel1 := context.WithCancel(ctx)
	done1 := make(chan struct{})
	go func() { defer close(done1); _ = srv1.Serve(serveCtx1) }()

	p, err := rpc.Open(ctx, addr)
	if err != nil {
		cancel1()
		_ = srv1.Close()
		<-done1
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	cancel1()
	_ = srv1.Close()
	<-done1

	// 再起一个同地址的新 server（新的空 mem 底座）。
	srv2, err := rpc.NewServer(ctx, rpc.ServerConfig{Addr: addr, Opener: memBackend(t)})
	if err != nil {
		t.Fatalf("重启 server: %v", err)
	}
	serveCtx2, cancel2 := context.WithCancel(ctx)
	done2 := make(chan struct{})
	go func() { defer close(done2); _ = srv2.Serve(serveCtx2) }()
	defer func() { cancel2(); _ = srv2.Close(); <-done2 }()

	// 给新监听一点时间就绪（同地址重绑）。
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = p.Set(ctx, "k2", []byte("v2"))
		if lastErr == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("服务端重启后客户端未能恢复: %v", lastErr)
	}
	// 新底座是空的，所以 k 读不到：这正说明连的确实是新 server。
	if _, ok, _ := p.Get(ctx, "k"); ok {
		t.Fatal("连上的应是重启后的新基座，k 不应存在")
	}
	if v, ok, _ := p.Get(ctx, "k2"); !ok || string(v) != "v2" {
		t.Fatalf("重启后写入的数据应可读, got %q,%v", v, ok)
	}
}

// kvOnly 是一个只实现 KvProvider 的最小基座，用来触发"能力未实现"路径。
type kvOnly struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (p *kvOnly) Set(ctx context.Context, key string, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.m == nil {
		p.m = map[string][]byte{}
	}
	p.m[key] = append([]byte(nil), value...)
	return nil
}

func (p *kvOnly) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	return p.Set(ctx, key, value)
}

func (p *kvOnly) SetExAt(ctx context.Context, key string, value []byte, at int64) error {
	if at <= core.NowUnix() {
		p.mu.Lock()
		delete(p.m, key)
		p.mu.Unlock()
		return nil
	}
	return p.Set(ctx, key, value)
}

func (p *kvOnly) Get(ctx context.Context, key string) ([]byte, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v, ok := p.m[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), v...), true, nil
}

func (p *kvOnly) Del(ctx context.Context, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.m, key)
	return nil
}

func (p *kvOnly) Exists(ctx context.Context, key string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.m[key]
	return ok, nil
}

func (p *kvOnly) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var cur int64
	if v, ok := p.m[key]; ok {
		n, err := parseInt64(v)
		if err != nil {
			return 0, core.ErrNotInteger
		}
		cur = n
	}
	cur += delta
	p.m[key] = []byte(fmt.Sprintf("%d", cur))
	return cur, nil
}

func (p *kvOnly) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	out := map[string][]byte{}
	for _, k := range keys {
		if v, ok, _ := p.Get(ctx, k); ok {
			out[k] = v
		}
	}
	return out, nil
}

func (p *kvOnly) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	if limit <= 0 {
		limit = core.DefaultScanLimit
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []core.KeyValue
	for k, v := range p.m {
		if start != "" && k < start {
			continue
		}
		if end != "" && k > end {
			continue
		}
		out = append(out, core.KeyValue{Key: k, Value: append([]byte(nil), v...)})
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (p *kvOnly) Expire(ctx context.Context, key string, ttl int64) error {
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	return nil
}

func (p *kvOnly) ExpireAt(_ context.Context, key string, at int64) error {
	if at <= core.NowUnix() {
		p.mu.Lock()
		delete(p.m, key)
		p.mu.Unlock()
	}
	return nil
}

func (p *kvOnly) TTL(ctx context.Context, key string) (int64, bool, error) {
	return 0, false, nil
}

// mustURL 解析测试用 URI。
func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u
}

func parseInt64(b []byte) (int64, error) {
	return strconv.ParseInt(string(b), 10, 64)
}
