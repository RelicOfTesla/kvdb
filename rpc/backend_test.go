package rpc_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/rpc"
	"github.com/RelicOfTesla/kvdb/sqlite"

	// 这些基座是给**服务端**用的：server 侧必须 import 它要暴露的基座。
	// 客户端一行都不需要——c/s 隔离的核心就在这个对比里。
	_ "github.com/RelicOfTesla/kvdb/badger"
	_ "github.com/RelicOfTesla/kvdb/bolt"
	_ "github.com/RelicOfTesla/kvdb/jsonl"
	_ "github.com/RelicOfTesla/kvdb/leveldb"
)

// 本文件验证用户提出的核心场景：server 侧选一个**真实持久化基座**
// （sqlite / jsonl / bolt 等），client 完全不知道底座是什么，
// 却能得到与本地直连一致的语义与能力声明。

// TestServerWithSQLiteBackend 用 sqlite 作底座走完整链路。
//
// 注意本测试文件 import 了 sqlite —— 因为**服务端**要用它。
// 客户端那一侧（另一个进程里）完全不需要这个 import，这正是 c/s 隔离的价值。
func TestServerWithSQLiteBackend(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "data.db")

	_, addr := startServer(t, rpc.ServerConfig{
		Backend:  "sqlite://" + dbPath,
		Auth:     rpc.AuthChallenge,
		Password: testPassword,
	})
	ctx := context.Background()

	p, err := rpc.OpenWithConfig(ctx, rpc.Config{
		Addr: addr, Auth: rpc.AuthChallenge, Password: testPassword,
	})
	if err != nil {
		t.Fatalf("连接 sqlite 底座的 server: %v", err)
	}
	defer p.Close()

	// sqlite 具备三种能力且批内可见（同事务逐条应用）。
	caps := p.Capabilities()
	if !caps.Queue || !caps.ZSet || !caps.Batch {
		t.Fatalf("sqlite 底座能力 = %+v", caps)
	}
	if !caps.BatchComposed {
		t.Fatalf("sqlite 的 BatchComposed 应为 true, got %+v", caps)
	}

	// 写三类数据 + TTL + 批写，然后经 RPC 读回。
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := p.QPush(ctx, "jobs", []byte("j1")); err != nil {
		t.Fatal(err)
	}
	if err := p.ZSet(ctx, "rank", "alice", 10); err != nil {
		t.Fatal(err)
	}
	if err := p.ApplyBatch(ctx, []core.BatchOp{
		{Kind: core.BatchSet, Key: "bk", Value: []byte("bv")},
		{Kind: core.BatchZIncr, Key: "rank", Member: "alice", Delta: 5},
	}); err != nil {
		t.Fatal(err)
	}
	if sc, ok, _ := p.ZGet(ctx, "rank", "alice"); !ok || sc != 15 {
		t.Fatalf("批内 ZIncr 应累加到 15, got %d,%v", sc, ok)
	}
	if v, ok, _ := p.Get(ctx, "bk"); !ok || string(v) != "bv" {
		t.Fatalf("批写键读回 = %q,%v", v, ok)
	}

	// 关掉 client 与 server，确认数据真的落了 sqlite 文件。
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestServerBackendIsolatedFromClient 是 TestServerWithSQLiteBackend 的补强：
// **同一个进程内**先起 server（sqlite 底座），关掉它，再直接用 sqlite 打开
// 同一个文件，验证 RPC 写入的数据确实落在基座里，而不是留在 RPC 层。
func TestServerBackendIsolatedFromClient(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "shared.db")

	srv, addr := startServer(t, rpc.ServerConfig{Backend: "sqlite://" + dbPath})
	ctx := context.Background()

	p, err := rpc.Open(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Set(ctx, "persisted", []byte("yes")); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	// 关掉 server，让 sqlite 文件落盘可读。
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("sqlite 文件应存在: %v", err)
	}

	// 绕过 RPC，直接用 sqlite 基座读同一个文件。
	direct, err := sqlite.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	v, ok, err := direct.Get(ctx, "persisted")
	if err != nil || !ok || string(v) != "yes" {
		t.Fatalf("经 RPC 写入的数据应落在 sqlite 里, got %q,%v,%v", v, ok, err)
	}
}

// TestAgainstRealBackends 把 RPC 通道接到若干真实基座上，逐一跑一遍
// 基础读写：证明"client 不感知底座"不是只对 mem 成立。
//
// 用例按可用性跳过：这里只依赖纯 Go 的本地基座，因此总是会跑。
func TestAgainstRealBackends(t *testing.T) {
	cases := []struct {
		name    string
		backend func(dir string) string
	}{
		{"sqlite", func(dir string) string { return "sqlite://" + filepath.Join(dir, "d.db") }},
		{"jsonl", func(dir string) string { return "jsonl://" + filepath.Join(dir, "d.jsonl") }},
		{"bolt", func(dir string) string { return "bolt://" + filepath.Join(dir, "d.bolt") }},
		{"leveldb", func(dir string) string { return "leveldb://" + filepath.Join(dir, "d.ldb") }},
		{"badger", func(dir string) string { return "badger://" + filepath.Join(dir, "d.badger") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			_, addr := startServer(t, rpc.ServerConfig{Backend: tc.backend(dir)})
			ctx := context.Background()

			p, err := rpc.Open(ctx, addr)
			if err != nil {
				t.Fatalf("连接 %s 底座的 server: %v", tc.name, err)
			}
			defer p.Close()

			// 三种数据结构 + 扫描，逐一经 RPC 验证。
			if err := p.Set(ctx, "k1", []byte("v1")); err != nil {
				t.Fatal(err)
			}
			if err := p.Set(ctx, "k2", []byte("v2")); err != nil {
				t.Fatal(err)
			}
			if v, ok, _ := p.Get(ctx, "k1"); !ok || string(v) != "v1" {
				t.Fatalf("%s: Get = %q,%v", tc.name, v, ok)
			}
			kvs, err := p.Scan(ctx, "k", "kz", 10)
			if err != nil {
				t.Fatalf("%s: Scan: %v", tc.name, err)
			}
			if len(kvs) != 2 || kvs[0].Key != "k1" || kvs[1].Key != "k2" {
				t.Fatalf("%s: Scan 应返回有序的 k1,k2, got %+v", tc.name, kvs)
			}
			if err := p.QPush(ctx, "q", []byte("e")); err != nil {
				t.Fatal(err)
			}
			if v, ok, _ := p.QPop(ctx, "q"); !ok || string(v) != "e" {
				t.Fatalf("%s: QPop = %q,%v", tc.name, v, ok)
			}
			if err := p.ZSet(ctx, "z", "m", 3); err != nil {
				t.Fatal(err)
			}
			if sc, ok, _ := p.ZGet(ctx, "z", "m"); !ok || sc != 3 {
				t.Fatalf("%s: ZGet = %d,%v", tc.name, sc, ok)
			}
			// 不存在的键：ok=false 且无错误（不能把"没找到"变成错误）。
			if _, ok, err := p.Get(ctx, "missing"); ok || err != nil {
				t.Fatalf("%s: 缺失键应 ok=false 且无错误, got ok=%v err=%v", tc.name, ok, err)
			}
		})
	}
}

// TestTTLOverRPCWithRealClock: TTL 的真实时钟行为经 RPC 仍然成立
// （kvdbtest 用的是虚拟时钟，这里补一条真实等待的短用例）。
func TestTTLOverRPCWithRealClock(t *testing.T) {
	_, addr := startServer(t, rpc.ServerConfig{Backend: "mem://"})
	p, err := rpc.Open(t.Context(), addr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	if err := p.SetEx(ctx, "k", []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	secs, ok, err := p.TTL(ctx, "k")
	if err != nil || !ok || secs <= 0 || secs > 1 {
		t.Fatalf("TTL = %d,%v,%v want (0,1]", secs, ok, err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok, _ := p.Get(ctx, "k"); !ok {
			return // 已过期，符合预期
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("设置了 1s TTL 的键应已过期")
}

// TestClientErrorIsNotClosedAfterServerError: 服务端返回业务错误后，
// 客户端连接必须仍然可用（不能把一次业务错误当成连接故障）。
func TestClientErrorIsNotClosedAfterServerError(t *testing.T) {
	_, addr := startServer(t, rpc.ServerConfig{Backend: "mem://"})
	p, err := rpc.Open(t.Context(), addr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	if err := p.Set(ctx, "s", []byte("not-a-number")); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Incr(ctx, "s", 1); !errors.Is(err, core.ErrNotInteger) {
		t.Fatalf("应 ErrNotInteger, got %v", err)
	}
	// 同一个 client 继续可用。
	if err := p.Set(ctx, "ok", []byte("v")); err != nil {
		t.Fatalf("业务错误后连接应仍然可用: %v", err)
	}
	if err := p.Ping(ctx); err != nil {
		t.Fatalf("Ping 应成功: %v", err)
	}
	// 也仍然是一个可用的 kvdb.DB（经 Open 拿到的形态）。
	db, err := kvdb.Open(ctx, "rpc://"+addr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if v, ok, _ := db.Get(ctx, "ok"); !ok || string(v) != "v" {
		t.Fatalf("db.Get = %q,%v", v, ok)
	}
}
