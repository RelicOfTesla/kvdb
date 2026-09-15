// Command rpcserver 把任意 kvdb 基座暴露成 RPC 服务端，配合 client 实现
// "本地变远程"的 c/s 隔离。
//
// 用法：
//
//	# 最简：jsonl 底座 + 挑战认证（推荐）
//	go run ./rpcserver -addr :7788 -backend jsonl://./tmp/data.jsonl \
//	    -auth challenge -password s3cret
//
//	# 本地/内网裸奔（无认证、无 TLS）
//	go run ./rpcserver -addr :7788 -backend mem://
//
//	# TLS + 挑战认证（生产组合）
//	go run ./rpcserver -addr :7788 -backend sqlite://./tmp/data.db \
//	    -auth challenge -password s3cret -tls-cert ./server.pem -tls-key ./server.key
//
// 客户端（另一台机器上也一样，且**不需要** import 任何基座包）：
//
//	db, _ := kvdb.Open(ctx, "rpc://host:7788?auth=challenge&password=s3cret")
//	defer db.Close()
//	db.Set(ctx, "k", []byte("v"))
//
// 它单独成一个模块（而不是放进 rpc/），因为要 import .../all 来一次性接入
// 全部底座，而 rpc 本身刻意不依赖任何基座——那是客户端侧"不感知底座"的前提。
// 也因此：想裁剪底座时，把这里的 all 换成具体基座包即可。
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/RelicOfTesla/kvdb/rpc"

	// 本命令演示"服务端选择底座"：可按需只 import 要用的基座。
	// 换成 all 就一次接入全部内置基座。
	_ "github.com/RelicOfTesla/kvdb/all"
)

func main() {
	var (
		addr     = flag.String("addr", "127.0.0.1:7788", "监听地址 host:port")
		backend  = flag.String("backend", "mem://", "服务端底座 URI（mem/jsonl/bolt/leveldb/badger/sqlite/mysql/pg/redis/ssdb）")
		authMode = flag.String("auth", "none", "c/s 认证模式：none | plain | challenge")
		password = flag.String("password", "", "认证口令（plain/challenge 模式必填）")
		tlsCert  = flag.String("tls-cert", "", "TLS 证书文件（与 -tls-key 同时给出即启用 TLS）")
		tlsKey   = flag.String("tls-key", "", "TLS 私钥文件")
		codec    = flag.String("codec", "resp", "报文编解码：resp（默认，仿 Redis 协议）| binary")
		maxConns = flag.Int("max-conns", 0, "最大并发连接数（0 不限）")
	)
	flag.Parse()

	mode, err := rpc.ParseAuthMode(*authMode)
	if err != nil {
		log.Fatalf("参数错误: %v", err)
	}
	if mode != rpc.AuthNone && *password == "" {
		log.Fatalf("参数错误: -auth %s 需要 -password", mode)
	}
	if mode == rpc.AuthPlain {
		log.Printf("警告: plain 模式会把口令明文发到网络上；除非已启用 TLS 或走 unix socket，建议改用 -auth challenge")
	}

	cfg := rpc.ServerConfig{
		Network:  "tcp",
		Addr:     *addr,
		Backend:  *backend,
		Auth:     mode,
		Password: *password,
		MaxConns: *maxConns,
	}
	switch *codec {
	case "resp", "":
		cfg.Codec = rpc.CodecRESP
	case "binary":
		cfg.Codec = rpc.CodecBinary
	default:
		log.Fatalf("参数错误: 未知 codec %q", *codec)
	}

	// 文件型底座：先确保父目录存在（基座不自动建目录）。
	if u, err := url.Parse(*backend); err == nil && u.Scheme != "" {
		if p := u.Path; p != "" && u.Host == "" {
			if dir := filepath.Dir(p); dir != "" && dir != "." {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					log.Fatalf("创建目录 %s: %v", dir, err)
				}
			}
		}
	}

	if (*tlsCert == "") != (*tlsKey == "") {
		log.Fatal("参数错误: -tls-cert 与 -tls-key 必须成对给出")
	}
	if *tlsCert != "" {
		pair, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			log.Fatalf("加载 TLS 证书: %v", err)
		}
		cfg.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{pair},
			MinVersion:   tls.VersionTLS12,
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv, err := rpc.NewServer(ctx, cfg)
	if err != nil {
		log.Fatalf("启动失败: %v", err)
	}
	tlsDesc := "明文"
	if cfg.TLSConfig != nil {
		tlsDesc = "TLS"
	}
	log.Printf("kvdb RPC 服务端已启动 addr=%s 底座=%s codec=%s 认证=%s 传输=%s",
		srv.Addr(), *backend, *codec, mode, tlsDesc)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()

	select {
	case err := <-errCh:
		if err != nil {
			log.Fatalf("服务异常退出: %v", err)
		}
	case <-ctx.Done():
		log.Print("收到退出信号，正在关闭…")
	}
	// 给在途连接一点收尾时间。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = srv.Close(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
	}
	fmt.Println("已退出")
}
