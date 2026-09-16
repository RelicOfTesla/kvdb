// Package core 定义 kvdb 的持久化契约：Provider 接口、可选能力接口、
// 哨兵错误与共享常量。本包不依赖任何基座实现，基座包（kvdb/mem、kvdb/ssdb 等）
// 仅引用本包，从而避免根包聚合注册表与基座包之间的循环导入。
// 根包 kvdb 通过类型/变量别名原样再导出这些符号，业务代码 import "github.com/RelicOfTesla/kvdb" 即可。
package core

import (
	"context"
	"errors"
)

// DefaultScanLimit 是 Scan 在 limit<=0 时采用的页大小，避免无上限全表扫描。
const DefaultScanLimit = 100

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
	// ErrInvalidKey 表示 key/队列名/zset 名为空串。
	//
	// 契约把空 key 定义为**非法**，且读写路径**一致拒绝**：只拒写不拒读会留下
	// "写不进去却读得到"的自相矛盾状态，调用方无从判断。各基座在实现里显式调用
	// core.CheckKey（见 helper.go），故绕过 DB 适配层直接用 Provider 时同样成立。
	// 空串不在"删除集合（无 TTL/无成员）"的语义内，因此不会与 ErrNotFound 混淆。
	ErrInvalidKey = errors.New("kvdb: key must not be empty")
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
	// Set 写入 key 的 value；**不改变 key 已存在的 TTL**。
	// 取 SSDB set 语义（set 不动 ttl 表）。注意与 Redis SET **不同**：
	// Redis SET 默认清除 TTL，需显式 KEEPTTL 才保留（Redis 基座内部即用
	// KEEPTTL 对齐本语义，见 redis 包）。
	Set(ctx context.Context, key string, value []byte) error
	// SetEx 写入 value 并设置 ttl 秒存活（对应 Redis SETEX / SSDB setx，
	// 覆盖 key 既有 TTL）；ttl<=0 返回 ErrInvalidTTL。
	SetEx(ctx context.Context, key string, value []byte, ttl int64) error
	// SetExAt 写入 value，并让 key 在 at（unix 秒）过期——绝对时间版本，
	// 对应 Redis SETEXAT（6.2+）。at 已是过去时间时不写入，而是**删除该 key**
	//（与 Redis SETEXAT 对过去时间点的行为一致）。
	//
	// 与 SetEx(ttl) 的关系：SetEx(ttl) 等价于 SetExAt(Now().Unix()+ttl)，
	// 区别只在"到期时刻由谁算"——跨进程/重启后要保持同一到期时刻时用本方法。
	SetExAt(ctx context.Context, key string, value []byte, at int64) error

	// Get 读取 key；ok=false 表示 key 不存在（含已过期）。
	Get(ctx context.Context, key string) (value []byte, ok bool, err error)
	// Del 删除 key；key 不存在时不视为错误。
	Del(ctx context.Context, key string) error
	// Exists 判断 key 是否存在（已过期视为不存在）。
	Exists(ctx context.Context, key string) (bool, error)
	// Incr 原子地对 key 存储的十进制整数加 delta（key 不存在按 0 起算）；
	// 已有值非整数返回 ErrNotInteger。溢出行为按基座分歧：SQL/Redis 返回错误，
	// 内存型基座（mem/bolt/jsonl/leveldb/badger）与 SSDB（服务端对溢出不做检查）
	// 按 int64 回绕——契约不对溢出语义做统一承诺，经 Caps.IncrWraps 显式探测。
	Incr(ctx context.Context, key string, delta int64) (int64, error)
	// MGet 批量读取；结果只含存在的 key，不保证顺序。
	MGet(ctx context.Context, keys ...string) (map[string][]byte, error)
	// Scan 返回 start<=key<=end（字节序闭区间）的前 limit 个键值对；
	// start/end 为空串表示对应侧不限；limit<=0 按 DefaultScanLimit。
	// 返回按 key 升序。
	//
	// 与 Redis SCAN **无共同点**（仅名字相近）：Redis SCAN 是游标式遍历，
	// 不保证顺序、只保证有限次遍历内返回全部元素；这里是确定性的闭区间范围
	// 查询。更接近 SSDB 的 scan（开区间 + limit），本契约统一为闭区间。
	Scan(ctx context.Context, start, end string, limit int) ([]KeyValue, error)
	// Expire 设置 key 的存活秒数（ttl>0）；key 不存在时不视为错误
	// （基座按各自语义对齐：SSDB/Redis 均返回 ok）。
	Expire(ctx context.Context, key string, ttl int64) error
	// ExpireAt 让 key 在 at（unix 秒）过期——绝对时间版本，对应 Redis EXPIREAT。
	// at 已是过去时间时**立即删除该 key**（与 Redis EXPIREAT 一致）；
	// key 不存在时不视为错误。
	ExpireAt(ctx context.Context, key string, at int64) error
	// TTL 返回 key 剩余秒数与是否"存在有效 TTL"：
	// ok=false 表示 key 不存在、无 TTL 或已过期（SSDB ttl 对缺失/无 TTL 均返回 -1，
	// 各基座统一映射为 ok=false）；ok=true 时返回剩余秒数。
	//
	// 注意与 Redis TTL **不同**：Redis 用 -2 区分"key 不存在"、-1 区分"存在但无 TTL"，
	// 本契约把这两种情况都收敛为 ok=false，因此调用方无法再由返回值区分二者。
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

// Caps 描述一个基座实际具备的能力。用结构体而不是多返回值，是为了新增能力时
// 不必改动方法签名与所有调用点；零值表示"仅 KV"。
type Caps struct {
	// Queue 实现了 QueueProvider。
	Queue bool
	// ZSet 实现了 ZSetProvider。
	ZSet bool
	// Batch 实现了 BatchProvider（可一次提交一批操作）。
	Batch bool
	// BatchComposed 在 Batch 为 true 的前提下进一步表示：批内后续操作**能看到**
	// 本批前序操作的效果（同批覆盖同一 key、队列按声明顺序入队、ZSet+ZIncr 累加）。
	// 契约不要求该性质，取决于基座机制：在同一事务/同一把锁内逐条应用的为 true；
	// Redis / SSDB 这类则不承诺，可经 BatchComposedProvider 探测。
	BatchComposed bool
	// IncrWraps 声明 Incr 的溢出语义：true = 按 int64 回绕（内存型基座
	// mem/jsonl/bolt/leveldb/badger 与 SSDB 的自然行为）；false = 溢出按
	// ErrNotInteger 报错（sqlstore/Redis 有显式溢出检查）。两种语义均符合契约，
	// 业务在跨基座迁移"大计数器"场景时可据此分支；可经 IncrWrapsProvider 探测。
	IncrWraps bool
}

// BatchProvider 是可选的批量写能力：把一批操作以一次提交发出，降低往返与
// 持久化开销（SQL 一次事务一次 fsync、Redis 一次 MULTI/EXEC、SSDB 一次流水线、
// jsonl 一次 flush、LevelDB 一个原子 Batch）。收集逻辑由根包共享的 Batch 提供，
// 基座只需实现 ApplyBatch。
//
// 提交语义（契约只覆盖这一条）：
//   - 空批为空操作；提交成功即整批写入完成。
//
// **批内组合结果不属于契约**：同批多条操作涉及同一 key / 同一队列 / 同一 zset 成员
// 时，最终值取决于基座的执行机制，允许各基座不同——
//   - 在同一事务或同一把锁内逐条应用的基座（mem / jsonl / bolt / leveldb /
//     sqlite / mysql / pg / badger）天然能读到本批前序操作的效果，表现为
//     "后者覆盖前者""队列按声明顺序入队""ZSet+ZIncr 累加"。其中 leveldb 的
//     Batch 本身只是写缓冲，批内可见性由基座在批内维护"本批状态"
//     （batchState 本地合成队列计数与成员分数）后补齐；
//   - Redis 的 MULTI/EXEC、SSDB 的流水线同理不保证批内可见性。
//
// 需要确定性的组合结果时，把相互依赖的操作拆到不同批次或改用单键操作。
// 是否具备批内可见性可由 BatchComposedProvider 探测。
//
// 批内不含 Incr/QPop 这类依赖当前状态的操作（需先校验再写），如需请单独调用。
type BatchProvider interface {
	ApplyBatch(ctx context.Context, ops []BatchOp) error
}

// BatchComposedProvider 是可选的能力声明：实现它表示该基座的 ApplyBatch **保证**
// 批内后续操作能看到本批前序操作的效果（批内 read-your-writes）。
//
// 未实现该接口的基座不承诺这一点——同一批在两个基座上可能得到不同的终值。
// 业务据此决定"能否安全地把相互依赖的操作放进同一批"：
//
//	if _, ok := kvdb.Unwrap(db).(core.BatchComposedProvider); ok { /* 可组合 */ }
type BatchComposedProvider interface {
	// BatchComposed 恒为 true，仅作能力标记：存在即代表批内可见。
	BatchComposed() bool
}

// IncrWrapsProvider 是可选的能力声明：实现它表示该基座的 Incr 在 int64 溢出时
// 按**回绕**处理（wrap-around，内存型基座与 SSDB 的自然行为）；未实现（或 Caps
// 未置位）表示溢出按 ErrNotInteger 报错（SQL/Redis 有显式溢出检查）。
// 同一契约允许两种语义（见 KvProvider.Incr 注释），这里是把差异显式化。
type IncrWrapsProvider interface {
	// IncrWraps 恒为 true，仅作能力标记。
	IncrWraps() bool
}

// StoreProvider 是三种数据结构的聚合能力接口（KV + Queue + ZSet），
// 不含 Batch 与生命周期：只要具备三种能力即可满足，是"一个存储"的完整读写面。
//
// kvdb.TypedDB 正面对照该接口（泛型壳只依赖它），因此任何满足 StoreProvider 的
// 类型都能直接被类型化：kvdb.Typed(p)。
// 需要连 Batch/Close 一起声明的完整基座用 FullProvider（它内嵌本接口）。
type StoreProvider interface {
	KvProvider
	QueueProvider
	ZSetProvider
}

// FullProvider 是集齐全部能力与生命周期的"完整基座"组合接口，
// 供实现 KV + Queue + ZSet + Batch + Closer 的基座整体声明（编译期校验），
// 或业务方按完整能力持有具体基座。
type FullProvider interface {
	StoreProvider
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
	// QRange 只读返回 [start, stop] 索引区间内的元素，方向为**队头 → 队尾**，
	// 索引 0 起、闭区间；负索引从末尾数（-1 为最后一个），与 ZRange 完全对称。
	//
	// 与 QFront/QBack（只看两端）和 QPop/QPopBack（取出即改源）不同，本方法只读且
	// 可按位置取，因此"要读全队列但不能改源"的场景（如迁移）用它即可。
	// 区间越界按可用范围裁剪（与 ZRange 同规矩），不报错；空队列返回空切片。
	QRange(ctx context.Context, name string, start, stop int64) ([][]byte, error)
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
	// ZRangeByScore 返回**分数落在闭区间 [min, max]** 内的成员。
	//
	// desc 只改变**遍历方向**（false=分数升序、true=分数降序），**不改变参数含义**：
	// 始终要求 min <= max，调用方不必在逆序时把两个参数对调（Redis 的
	// ZREVRANGEBYSCORE 要求传 max,min，容易写错；此处刻意不沿用那个约定）。
	// min > max 视为空区间，返回空且不报错。
	// limit > 0 时最多返回 limit 个（按遍历方向取前 limit 个）；limit <= 0 表示不限。
	//
	// 排序与 ZRange 一致：分数相同时按成员字节序升序（desc 时该次序**不翻转**，
	// 与 Redis 同分成员的行为一致）。
	ZRangeByScore(ctx context.Context, name string, min, max int64, limit int, desc bool) ([]ZItem, error)
	// ZIncr 原子地对成员分数加 delta（不存在按 0 起算），返回新分数。
	ZIncr(ctx context.Context, name, key string, delta int64) (int64, error)
}
