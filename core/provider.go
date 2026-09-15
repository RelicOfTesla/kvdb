// Package core 定义 kvdb 的持久化契约：Provider 接口、可选能力接口、
// 哨兵错误与共享常量。本包不依赖任何基座实现，基座包（kvdb/mem、kvdb/ssdb 等）
// 仅引用本包，从而避免根包聚合注册表与基座包之间的循环导入。
// 根包 kvdb 通过类型/变量别名原样再导出这些符号，业务代码 import "github.com/RelicOfTesla/kvdb" 即可。
package core

import (
	"context"
	"errors"
	"math"
)

// DefaultScanLimit 是 Scan 在 limit<=0 时采用的页大小，避免无上限全表扫描。
const DefaultScanLimit = 100

// AddTTL 返回 now+ttl 的饱和和：ttl 大到溢出 int64 时钳制到 MaxInt64，
// 避免各基座把 expire_at 包绕成负数（键立即过期或永不过期的分歧）。
func AddTTL(now, ttl int64) int64 {
	if ttl > 0 && now > math.MaxInt64-ttl {
		return math.MaxInt64
	}
	return now + ttl
}

// 哨兵错误：仅用于 errors.Is 判等，不携带额外状态。
var (
	// ErrUnsupported 表示当前基座未实现对应可选能力（queue/zset）。
	ErrUnsupported = errors.New("kvdb: capability not implemented by this provider")
	// ErrClosed 表示基座已 Close，不再接受操作。
	ErrClosed = errors.New("kvdb: provider is closed")
	// ErrNotInteger 表示 Incr 遇到的值无法解析为十进制 int64。
	ErrNotInteger = errors.New("kvdb: value is not an integer")
	// ErrInvalidTTL 表示 Expire/SetEx 传入了非正 TTL
	//（不同基座对 TTL<=0 语义分歧，统一拒绝）。
	ErrInvalidTTL = errors.New("kvdb: ttl must be positive")
	// ErrNotFound 表示 key/成员不存在：Get 族以 ok=false 表达缺失，
	// 经 D/DMust 合并为 (T, error) 形态时转为该哨兵。
	ErrNotFound = errors.New("kvdb: key not found")
)

// KeyValue 是 Scan 返回的一个键值对。
type KeyValue struct {
	Key   string
	Value []byte
}

// KvProvider 是所有持久化基座必须实现的 KV 合同（以 SSDB/Redis 核心命令为原型）。
// 生命周期（Close）不在本接口内：基座如由 SDK 独占资源，另行实现 Closer，
// 由适配器 DB.Close 断言调用；也可由业务方自行管理。
// 三种数据结构的命名空间彼此独立。
type KvProvider interface {
	// Set 写入 key 的 value；不改变 key 已存在的 TTL（与 SSDB set / Redis SET 一致）。
	Set(ctx context.Context, key string, value []byte) error
	// SetEx 写入 value 并设置 ttl 秒存活（对应 Redis SETEX / SSDB setx，
	// 覆盖 key 既有 TTL）；ttl<=0 返回 ErrInvalidTTL。
	SetEx(ctx context.Context, key string, value []byte, ttl int64) error

	// Get 读取 key；ok=false 表示 key 不存在（含已过期）。
	Get(ctx context.Context, key string) (value []byte, ok bool, err error)
	// Del 删除 key；key 不存在时不视为错误。
	Del(ctx context.Context, key string) error
	// Exists 判断 key 是否存在（已过期视为不存在）。
	Exists(ctx context.Context, key string) (bool, error)
	// Incr 原子地对 key 存储的十进制整数加 delta（key 不存在按 0 起算）；
	// 已有值非整数返回 ErrNotInteger。溢出行为按基座分歧：SQL/Redis/SSDB
	// 返回错误（SSDB/PG 统一映射为 ErrNotInteger），内存型基座（mem/bolt/jsonl）
	// 按 int64 回绕——契约不对溢出语义做统一承诺。
	Incr(ctx context.Context, key string, delta int64) (int64, error)
	// MGet 批量读取；结果只含存在的 key，不保证顺序。
	MGet(ctx context.Context, keys ...string) (map[string][]byte, error)
	// Scan 返回 start<=key<=end（字节序闭区间）的前 limit 个键值对；
	// start/end 为空串表示对应侧不限；limit<=0 按 DefaultScanLimit。
	// 返回按 key 升序。
	Scan(ctx context.Context, start, end string, limit int) ([]KeyValue, error)
	// Expire 设置 key 的存活秒数（ttl>0）；key 不存在时不视为错误
	// （基座按各自语义对齐：SSDB/Redis 均返回 ok）。
	Expire(ctx context.Context, key string, ttl int64) error
	// TTL 返回 key 剩余秒数与是否"存在有效 TTL"：
	// ok=false 表示 key 不存在、无 TTL 或已过期（SSDB ttl 对缺失/无 TTL 均返回 -1，
	// 各基座统一映射为 ok=false）；ok=true 时返回剩余秒数。
	TTL(ctx context.Context, key string) (seconds int64, ok bool, err error)
}

// Closer 是可选的资源释放能力：独立于 KvProvider，由持有独占资源的基座实现；
// 未实现时适配器 DB.Close 为空操作（生命周期由业务方管理）。
type Closer interface {
	Close() error
}

// BatchOpKind 标识批内操作类型。批内只允许"无条件写"——它们不依赖键的当前
// 状态，因此可以先写日志/先入缓冲，再在提交时统一生效。
type BatchOpKind uint8

const (
	BatchSet        BatchOpKind = iota + 1 // Key, Value
	BatchSetEx                             // Key, Value, TTL
	BatchDel                               // Key
	BatchExpire                            // Key, TTL
	BatchQPush                             // Key(队列名), Value
	BatchQPushFront                        // Key(队列名), Value
	BatchZSet                              // Key(zset 名), Member, Score
	BatchZDel                              // Key, Member
	BatchZIncr                             // Key, Member, Delta
)

// BatchOp 是一条待批量提交的写操作。字段按 Kind 取用，未用字段忽略。
type BatchOp struct {
	Kind   BatchOpKind
	Key    string // kv key / 队列名 / zset 名
	Member string // zset 成员
	Value  []byte
	TTL    int64 // SetEx / Expire
	Score  int64 // ZSet
	Delta  int64 // ZIncr
}

// BatchProvider 是可选的批量写能力：把一批操作以一次提交发出，降低往返与
// 持久化开销（SQL 一次事务一次 fsync、Redis 一次 MULTI/EXEC、SSDB 一次流水线、
// jsonl 一次 flush）。收集逻辑由根包共享的 Batch 提供，基座只需实现 ApplyBatch。
//
// 提交语义：
//   - 整批按 ops 顺序生效；空批为空操作；
//   - 提交失败时：具备事务能力的基座（mysql/sqlite/pg/jsonl/mem）保证整批
//     不生效；Redis 走 MULTI/EXEC，网络/协议错误整批不生效，但命令级错误
//     （如 WRONGTYPE/OOM）时 EXEC 不回滚，之前的命令可能已生效；SSDB 无事务，
//     采用流水线，失败时可能部分生效（其价值在于减少往返）；
//   - 批内不含 Incr/QPop 这类依赖当前状态的操作（需先校验再写），如需请单独调用。
type BatchProvider interface {
	ApplyBatch(ctx context.Context, ops []BatchOp) error
}

// FullProvider 是集齐全部能力与生命周期的"完整基座"组合接口，
// 供实现 KV + Queue + ZSet + Batch + Closer 的基座整体声明（编译期校验），
// 或业务方按完整能力持有具体基座。
type FullProvider interface {
	KvProvider
	QueueProvider
	ZSetProvider
	BatchProvider
	Closer
}

// QueueProvider 是可选的队列能力（原型：SSDB qpush*/qpop* + Redis List）。
// 队列为先进先出语义：QPush 追加到队尾，QPop 从队头取出。
type QueueProvider interface {
	// QPush 追加 value 到队尾。
	QPush(ctx context.Context, name string, value []byte) error
	// QPushFront 插入 value 到队头。
	QPushFront(ctx context.Context, name string, value []byte) error
	// QPop 取出并移除队头；ok=false 表示队列为空。
	QPop(ctx context.Context, name string) (value []byte, ok bool, err error)
	// QPopBack 取出并移除队尾；ok=false 表示队列为空。
	QPopBack(ctx context.Context, name string) (value []byte, ok bool, err error)
	// QSize 返回队列长度。
	QSize(ctx context.Context, name string) (int64, error)
	// QFront 只读查看队头；ok=false 表示队列为空。
	QFront(ctx context.Context, name string) (value []byte, ok bool, err error)
	// QBack 只读查看队尾；ok=false 表示队列为空。
	QBack(ctx context.Context, name string) (value []byte, ok bool, err error)
}

// ZItem 是 ZRange 返回的一个成员及其分数。
type ZItem struct {
	Key   string
	Score int64
}

// ZSetProvider 是可选的 sorted-set 能力（原型：SSDB zset* + Redis Z*）。
// 分数类型为 int64（SSDB 原生即 int64；Redis 基座在 |score|<2^53 内无损换算）。
// 排序为 (score 升序, key 升序)。
type ZSetProvider interface {
	// ZSet 写入成员 key 的分数 score（不存在则创建）。
	ZSet(ctx context.Context, name, key string, score int64) error
	// ZGet 读取成员分数；ok=false 表示成员不存在。
	ZGet(ctx context.Context, name, key string) (score int64, ok bool, err error)
	// ZDel 删除成员。
	ZDel(ctx context.Context, name, key string) error
	// ZSize 返回集合成员数。
	ZSize(ctx context.Context, name string) (int64, error)
	// ZRank 返回成员升序排名（0 起）；ok=false 表示成员不存在。
	ZRank(ctx context.Context, name, key string) (rank int64, ok bool, err error)
	// ZRange 返回 [start, stop] 索引区间（0 起闭区间）的成员，按升序；
	// 负索引表示从末尾数（-1 为最后一个），与 Redis ZRANGE 一致。
	ZRange(ctx context.Context, name string, start, stop int64) ([]ZItem, error)
	// ZIncr 原子地对成员分数加 delta（不存在按 0 起算），返回新分数。
	ZIncr(ctx context.Context, name, key string, delta int64) (int64, error)
}
