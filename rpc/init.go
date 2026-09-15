package rpc

import "github.com/RelicOfTesla/kvdb"

// init 把 RPC 客户端注册进 kvdb 的 scheme 注册表，使
// kvdb.Open(ctx, "rpc://host:port?...") 可用。
//
// 注意：注册的只是**客户端**。服务端需要显式 rpc.NewServer，并且
// server 侧 import 的基座包与 client 完全无关——这正是 c/s 隔离的意义。
//
// 注册独立成本文件，与其他基座包的 init.go 保持一致：接入点集中醒目，
// 也避免实现文件为了 MustRegister 而依赖根包。
func init() { kvdb.MustRegister("rpc", OpenURI) }
