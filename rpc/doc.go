// Package rpc 提供一个"本地变远程"的 c/s 模式：server 侧选定任意基座
// （mem / jsonl / bolt / leveldb / badger / sqlite / mysql / pg / redis / ssdb 或
// 自定义注册的），client 侧通过 RPC 使用它，两侧都只依赖 core 契约。
//
// 三个边界：
//
//   - **client 不感知 server 底座**：client 实现的是 core.FullProvider，与本地基座
//     同一组接口；它不知道对端是 jsonl 还是 mysql，也不需要知道。
//   - **认证是 c/s 协议自己的事**：独立于底座自身的 auth（如 ssdb 的 server.auth），
//     支持 none / plain / challenge 三种模式，见 auth.go。
//   - **传输可 TLS 可不 TLS**：同一端口按 server 配置切换，client 用参数声明期望。
//
// 一个完整的用法：
//
//	// server
//	srv, _ := rpc.NewServer(rpc.ServerConfig{
//	    Network: "tcp", Addr: ":7788",
//	    Backend: "jsonl://./data.jsonl",
//	    Auth:    rpc.AuthChallenge, Password: "s3cret",
//	})
//	go srv.Serve(ctx)
//
//	// client（可与 server 同进程，也可在另一台机器）
//	db, _ := kvdb.Open(ctx, "rpc://127.0.0.1:7788?auth=challenge&password=s3cret")
//	defer db.Close()
//	db.Set(ctx, "k", []byte("v"))
//
// server 侧只需 import 一次所选基座包（或 all），client 侧完全不 import 任何基座。
package rpc
