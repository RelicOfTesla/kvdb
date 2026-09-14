// Package all 一次性注册全部内置基座，便于"全部可用"的宿主（示例、测试、
// 运维工具）一行接入：
//
//	import _ "kvdb/all"
//
// 正式服务建议按需 import 具体基座包（如 _ "kvdb/sqlite"），只引入用到的驱动。
package all

import (
	// 各基座包在 init 中通过 kvdb.MustRegister 注册自己的 scheme。
	_ "kvdb/jsonl"
	_ "kvdb/mem"
	_ "kvdb/mysql"
	_ "kvdb/pg"
	_ "kvdb/redis"
	_ "kvdb/sqlite"
	_ "kvdb/ssdb"
)
