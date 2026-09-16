package main

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/rpc"

	_ "github.com/RelicOfTesla/kvdb/all" // 与 main.go 一致：一次接入全部内置基座
)

// startServer 按 main.go 的装配方式起一个服务端（端口交给内核分配），
// 返回其 addr 与关闭函数。测试只走 main.go 用到的那些配置项，
// 因此这里验证的是"这个 demo 真能用"，而不是另写一套调用。
func startServer(t *testing.T, backend string, mode rpc.AuthMode, password string) (string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	srv, err := rpc.NewServer(ctx, rpc.ServerConfig{
		Network:  "tcp",
		Addr:     "127.0.0.1:0", // 0 = 内核分配，避免测试间抢端口
		Backend:  backend,
		Auth:     mode,
		Password: password,
		Codec:    rpc.CodecRESP, // 与 main.go 的默认值一致
	})
	if err != nil {
		cancel()
		t.Fatalf("NewServer: %v", err)
	}
	go func() { _ = srv.Serve(ctx) }()

	// 等服务端真正可连（Serve 在独立 goroutine 里跑）。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", srv.Addr(), 100*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return srv.Addr(), func() {
		_ = srv.Close()
		cancel()
	}
}

// TestRPCServerRoundTrip 端到端验证 demo 的默认组合（mem 底座 + 无认证）：
// 服务端把底座暴露成 RPC，客户端经网络完成一轮读写。
func TestRPCServerRoundTrip(t *testing.T) {
	addr, stop := startServer(t, "mem://", rpc.AuthNone, "")
	defer stop()

	ctx := context.Background()
	db, err := kvdb.Open(ctx, "rpc://"+addr)
	if err != nil {
		t.Fatalf("客户端 Open: %v", err)
	}
	defer db.Close()

	if err := db.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	v, ok, err := db.Get(ctx, "k")
	if err != nil || !ok || string(v) != "v" {
		t.Fatalf("Get = %q,%v,%v", v, ok, err)
	}
	// 计数器跨网络仍应原子累加
	if n, err := db.Incr(ctx, "n", 5); err != nil || n != 5 {
		t.Fatalf("Incr = %d, %v", n, err)
	}
	if n, err := db.Incr(ctx, "n", 2); err != nil || n != 7 {
		t.Fatalf("Incr = %d, %v", n, err)
	}
	// 缺失 key 仍走 ok=false（而非错误）
	if _, ok, err := db.Get(ctx, "missing"); err != nil || ok {
		t.Fatalf("缺失 key = ok:%v err:%v", ok, err)
	}
}

// TestRPCServerChallengeAuth 验证 main.go 的推荐认证组合（challenge）：
// 口令正确可读写；口令错误在 Open 阶段即失败（不会拖到第一条命令）。
func TestRPCServerChallengeAuth(t *testing.T) {
	addr, stop := startServer(t, "mem://", rpc.AuthChallenge, "s3cret")
	defer stop()

	ctx := context.Background()
	// 正确口令
	ok, err := kvdb.Open(ctx, "rpc://"+addr+"?auth=challenge&password=s3cret")
	if err != nil {
		t.Fatalf("正确口令应能 Open: %v", err)
	}
	if err := ok.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("认证后 Set: %v", err)
	}
	ok.Close()

	// 错误口令：Open 阶段应报错
	if bad, err := kvdb.Open(ctx, "rpc://"+addr+"?auth=challenge&password=wrong"); err == nil {
		bad.Close()
		t.Fatal("错误口令不应 Open 成功")
	}
}

// TestRPCServerFileBackend 验证 main.go 里"文件型底座 + 目录自动创建"那条路径：
// 经 RPC 写入的数据应当在服务端进程重启后仍然存在。
func TestRPCServerFileBackend(t *testing.T) {
	dir := t.TempDir()
	backend := "jsonl://" + dir + "/data.jsonl"

	addr, stop := startServer(t, backend, rpc.AuthNone, "")
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "rpc://"+addr)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Set(ctx, "persist", []byte("yes")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	db.Close()
	stop() // 服务端下线（底座的 Close 会落盘）

	// 重新起一个服务端指向同一份数据：数据应还在。
	addr2, stop2 := startServer(t, backend, rpc.AuthNone, "")
	defer stop2()
	db2, err := kvdb.Open(ctx, "rpc://"+addr2)
	if err != nil {
		t.Fatalf("重开 Open: %v", err)
	}
	defer db2.Close()
	if v, ok, err := db2.Get(ctx, "persist"); err != nil || !ok || string(v) != "yes" {
		t.Fatalf("重启后 Get = %q,%v,%v", v, ok, err)
	}
}

// TestBackendURIAccepted 验证 main.go 里列出的底座 URI 形态都能被服务端接受
// （只做启动校验，不必逐一跑通读写——各底座的完整契约由各自模块的用例覆盖）。
func TestBackendURIAccepted(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct{ name, uri string }{
		{"mem", "mem://"},
		{"jsonl", "jsonl://" + dir + "/a.jsonl"},
		{"bolt", "bolt://" + dir + "/a.bolt"},
		{"leveldb", "leveldb://" + dir + "/a.ldb"},
		{"badger", "badger://" + dir + "/a.badger"},
		{"sqlite", "sqlite://" + dir + "/a.db"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			srv, err := rpc.NewServer(ctx, rpc.ServerConfig{
				Network: "tcp", Addr: "127.0.0.1:0",
				Backend: tc.uri, Auth: rpc.AuthNone, Codec: rpc.CodecRESP,
			})
			if err != nil {
				t.Fatalf("底座 %s 启动失败: %v", tc.uri, err)
			}
			defer srv.Close()
			// NewServer 只完成 Listen，必须再 Serve 才会 Accept；
			// 不启动会在握手阶段表现为超时（而不是连不上），很容易误判成 codec 不匹配。
			go func() { _ = srv.Serve(ctx) }()

			db, err := kvdb.Open(ctx, "rpc://"+srv.Addr())
			if err != nil {
				t.Fatalf("客户端 Open: %v", err)
			}
			defer db.Close()
			if err := db.Set(ctx, "k", []byte(tc.name)); err != nil {
				t.Fatalf("Set: %v", err)
			}
			if v, ok, err := db.Get(ctx, "k"); err != nil || !ok || string(v) != tc.name {
				t.Fatalf("Get = %q,%v,%v", v, ok, err)
			}
		})
	}
}

// TestParseAuthModeRejectsUnknown 验证 main.go 启动时会因非法 -auth 直接失败
// （demo 的入参校验路径，避免"配错了却静默裸奔"）。
func TestParseAuthModeRejectsUnknown(t *testing.T) {
	if _, err := rpc.ParseAuthMode("bogus"); err == nil {
		t.Fatal("非法 auth 模式应报错")
	}
	for _, m := range []string{"none", "plain", "challenge"} {
		if _, err := rpc.ParseAuthMode(m); err != nil {
			t.Fatalf("%s 应被接受: %v", m, err)
		}
	}
	// 错误信息应列出可选值，便于排查
	if _, err := rpc.ParseAuthMode("bogus"); !strings.Contains(err.Error(), "challenge") {
		t.Fatalf("错误信息应提示可选模式, got %v", err)
	}
}
