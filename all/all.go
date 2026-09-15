// Package all 一次性注册全部内置基座，便于"全部可用"的宿主（示例、测试、
// 运维工具）一行接入：
//
//	import _ "github.com/RelicOfTesla/kvdb/all"
//
// 正式服务建议按需 import 具体基座包（如 _ "github.com/RelicOfTesla/kvdb/sqlite"），只引入用到的驱动。
package all

import (
	// 各基座包在 init 中通过 kvdb.MustRegister 注册自己的 scheme。
	_ "github.com/RelicOfTesla/kvdb/badger"
	_ "github.com/RelicOfTesla/kvdb/bolt"
	_ "github.com/RelicOfTesla/kvdb/jsonl"
	_ "github.com/RelicOfTesla/kvdb/leveldb"
	_ "github.com/RelicOfTesla/kvdb/mem"
	_ "github.com/RelicOfTesla/kvdb/mysql"
	_ "github.com/RelicOfTesla/kvdb/pg"
	_ "github.com/RelicOfTesla/kvdb/redis"
	// rpc 注册的是**客户端**：接入后可用 kvdb.Open("rpc://host:port") 连远端。
	// 它不引入任何基座依赖——服务端那边的底座由 server 自己 import。
	_ "github.com/RelicOfTesla/kvdb/rpc"
	_ "github.com/RelicOfTesla/kvdb/sqlite"
	_ "github.com/RelicOfTesla/kvdb/ssdb"
)
