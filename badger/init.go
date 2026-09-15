package badger

import "github.com/RelicOfTesla/kvdb"

// init 把本基座注册进 kvdb 的 scheme 注册表，使 kvdb.Open(ctx, "badger://…") 可用。
// 注册独立成本文件：接入点集中醒目、不掺进实现代码，也避免实现文件为了
// MustRegister 而依赖根包。本包被 import（含 _ 形式）即完成注册。
func init() { kvdb.MustRegister("badger", OpenURI) }
