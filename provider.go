// Package kvdb 提供一个以 redis/ssdb 语义为原型、可插拔持久化基座的
// kv + queue + zset 适配器 SDK。
//
// 核心抽象是 KvProvider 接口（KV 必选），Queue/ZSet 通过可选能力接口由具体
// 基座自行实现（见 QueueProvider / ZSetProvider）。适配器 DB 对三种数据结构
// 提供统一入口：基座未实现某能力时返回 ErrUnsupported，业务层可通过
// Capabilities 提前探测。通过 Register 注册的自定义基座同样遵守该约定。
// 资源释放由独立的 Closer 接口承载：基座未实现时 DB.Close 为空操作，
// 生命周期由业务方自行管理。
package kvdb

import (
	"context"

	"github.com/RelicOfTesla/kvdb/core"
)

// 再导出 core 契约符号，业务代码 import "github.com/RelicOfTesla/kvdb" 即可使用；errors.Is 判等
// 与常量身份均指向 core 中的同一实例。
type (
	KvProvider    = core.KvProvider
	FullProvider  = core.FullProvider
	QueueProvider = core.QueueProvider
	ZSetProvider  = core.ZSetProvider
	BatchProvider = core.BatchProvider
	Closer        = core.Closer
	KeyValue      = core.KeyValue
	ZItem         = core.ZItem
	BatchOp       = core.BatchOp
	BatchOpKind   = core.BatchOpKind
)

// 批量操作类型常量（core.BatchOpKind）。
const (
	BatchSet        = core.BatchSet
	BatchSetEx      = core.BatchSetEx
	BatchDel        = core.BatchDel
	BatchExpire     = core.BatchExpire
	BatchQPush      = core.BatchQPush
	BatchQPushFront = core.BatchQPushFront
	BatchZSet       = core.BatchZSet
	BatchZDel       = core.BatchZDel
	BatchZIncr      = core.BatchZIncr
)

const DefaultScanLimit = core.DefaultScanLimit

var (
	ErrUnsupported = core.ErrUnsupported
	ErrClosed      = core.ErrClosed
	ErrNotInteger  = core.ErrNotInteger
	ErrInvalidTTL  = core.ErrInvalidTTL
	ErrNotFound    = core.ErrNotFound
)

// Open 按 URI 的 scheme 选择基座并返回适配器接口 DB（注册表见 registry.go）。
// 返回接口而非具体类型，便于业务依赖并在测试中替换为 mock。
func Open(ctx context.Context, uri string) (DB, error) {
	return open(ctx, uri)
}
