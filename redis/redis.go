// Package redis 提供以 Redis 服务器为后端的基座，实现 KV（String）+ Queue（List）
// + ZSet（Sorted Set）三种能力。分数为 int64，经 float64 无损换算
// （|score|<=2^53 精确；超出会失真，SDK 契约按 SSDB int64 定义，README 有说明）。
// Scan 语义说明：Redis SCAN 不保证顺序，本实现全量游标扫描后在客户端过滤区间
// 并排序，代价与全 keyspace 大小相关。
package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

func init() { kvdb.MustRegister("redis", OpenURI) }

// OpenURI 解析 redis://[:pass]@host:port[/db]，亦支持 ?password= 传参。
func OpenURI(ctx context.Context, u *url.URL) (core.KvProvider, error) {
	cfg := Config{Addr: u.Host}
	if u.User != nil {
		if pw, ok := u.User.Password(); ok {
			cfg.Password = pw
		}
	}
	if cfg.Password == "" {
		cfg.Password = u.Query().Get("password")
	}
	cfg.KeyPrefix = u.Query().Get("key_prefix")
	if db := strings.TrimPrefix(u.Path, "/"); db != "" {
		if n, err := strconv.Atoi(db); err == nil {
			cfg.DB = n
		}
	}
	return Open(ctx, cfg)
}

var (
	_ core.FullProvider = (*Provider)(nil)
)

// 三类数据共存于同一个 Redis keyspace，而 core.KvProvider 约定"命名空间彼此独立"，
// 因此用前缀隔离：同名 KV 键 / 队列 / zset 互不干扰，Scan 也只扫描 KV 前缀
// （否则会在 List/Sorted Set 键上触发 WRONGTYPE）。
//
// DefaultKeyPrefix 是 Config.KeyPrefix 为空时采用的前缀。实际生效的三段前缀由
// 它派生：<prefix>kv: / <prefix>q: / <prefix>z:。
const DefaultKeyPrefix = "kvdb:"

// prefixes 是由 Config.KeyPrefix 派生的三段实际前缀。
type prefixes struct {
	kv, q, z string
}

func newPrefixes(base string) prefixes {
	if base == "" {
		base = DefaultKeyPrefix
	}
	return prefixes{kv: base + "kv:", q: base + "q:", z: base + "z:"}
}

// Config 是 Redis 连接配置。
type Config struct {
	Addr     string // host:port
	Username string
	Password string
	DB       int // 库号（0-15）
	PoolSize int // 0 使用驱动默认值
	// KeyPrefix 是键命名空间前缀，派生出 <pfx>kv: / <pfx>q: / <pfx>z: 三段；
	// 空串取 DefaultKeyPrefix。多个应用共用一个 Redis 库时用它互相隔离。
	KeyPrefix string
}

// Provider 是 Redis 基座。go-redis 内部自带连接池，并发安全。
type Provider struct {
	rd        *goredis.Client
	pfx       prefixes // 键命名空间前缀（见 Config.KeyPrefix）
	closed    atomic.Bool
	closeOnce sync.Once
}

// Open 建立连接并 Ping 确认可达。
func Open(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:6379"
	}
	rd := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Addr,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})
	if err := rd.Ping(ctx).Err(); err != nil {
		rd.Close()
		return nil, fmt.Errorf("redis: ping %s: %w", cfg.Addr, err)
	}
	return &Provider{rd: rd, pfx: newPrefixes(cfg.KeyPrefix)}, nil
}

// check 判断是否已关闭：closed 为 atomic.Bool，与并发 Close 无数据竞争。
func (p *Provider) check() error {
	if p.closed.Load() {
		return core.ErrClosed
	}
	return nil
}

// Close 幂等：用 sync.Once 保证底层连接池只关闭一次，重复调用安全。
func (p *Provider) Close() error {
	var err error
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		err = p.rd.Close()
	})
	return err
}

// ---- KV ----

func (p *Provider) Set(ctx context.Context, key string, value []byte) error {
	if err := p.check(); err != nil {
		return err
	}
	key = p.pfx.kv + key
	// KEEPTTL 保持既有 TTL（与 SSDB set 不触碰 ttl 表语义对齐），需 Redis >= 6.0。
	if err := p.rd.SetArgs(ctx, key, string(value), goredis.SetArgs{KeepTTL: true}).Err(); err != nil {
		return fmt.Errorf("redis: set: %w", err)
	}
	return nil
}

// SetEx 写入 value 并设置 ttl 秒存活（Redis SETEX；覆盖旧 TTL）。
func (p *Provider) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	if err := p.check(); err != nil {
		return err
	}
	key = p.pfx.kv + key
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	if err := p.rd.Set(ctx, key, string(value), secondsDuration(ttl)).Err(); err != nil {
		return fmt.Errorf("redis: setex: %w", err)
	}
	return nil
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := p.check(); err != nil {
		return nil, false, err
	}
	key = p.pfx.kv + key
	v, err := p.rd.Get(ctx, key).Result()
	if errors.Is(err, goredis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redis: get: %w", err)
	}
	return []byte(v), true, nil
}

func (p *Provider) Del(ctx context.Context, key string) error {
	if err := p.check(); err != nil {
		return err
	}
	key = p.pfx.kv + key
	if err := p.rd.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis: del: %w", err)
	}
	return nil
}

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	if err := p.check(); err != nil {
		return false, err
	}
	key = p.pfx.kv + key
	n, err := p.rd.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("redis: exists: %w", err)
	}
	return n > 0, nil
}

func (p *Provider) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	if err := p.check(); err != nil {
		return 0, err
	}
	key = p.pfx.kv + key
	n, err := p.rd.IncrBy(ctx, key, delta).Result()
	if err != nil {
		if notInteger(err) {
			return 0, core.ErrNotInteger
		}
		return 0, fmt.Errorf("redis: incr: %w", err)
	}
	return n, nil
}

func (p *Provider) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		// 与 mem/SQL 基座对齐：空 key 列表返回空 map（裸 MGET 会被服务端
		// 以 wrong number of arguments 拒绝）。
		return map[string][]byte{}, nil
	}
	rkeys := make([]string, len(keys))
	for i, k := range keys {
		rkeys[i] = p.pfx.kv + k
	}
	vals, err := p.rd.MGet(ctx, rkeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: mget: %w", err)
	}
	out := make(map[string][]byte, len(keys))
	for i, v := range vals {
		if v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		out[keys[i]] = []byte(s)
	}
	return out, nil
}

func (p *Provider) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	limit = normalizeLimit(limit)

	// 只扫描 KV 前缀（队列/zset 键不是 KV 条目，直接 GET 会 WRONGTYPE），
	// 剥离前缀后按用户可见 key 过滤区间；SCAN 无序，需全量收集后客户端排序。
	var found []string
	cur := uint64(0)
	for {
		keys, next, err := p.rd.Scan(ctx, cur, p.pfx.kv+"*", int64(ScanCount)).Result()
		if err != nil {
			return nil, fmt.Errorf("redis: scan: %w", err)
		}
		for _, rk := range keys {
			k := strings.TrimPrefix(rk, p.pfx.kv)
			if start != "" && k < start {
				continue
			}
			if end != "" && k > end {
				continue
			}
			found = append(found, k)
		}
		cur = next
		if cur == 0 {
			break
		}
	}
	sort.Strings(found)
	if len(found) > limit {
		found = found[:limit]
	}
	out := make([]core.KeyValue, 0, len(found))
	for _, k := range found {
		v, err := p.rd.Get(ctx, p.pfx.kv+k).Result()
		if errors.Is(err, goredis.Nil) {
			continue // 扫描期间被删，跳过
		}
		if err != nil {
			return nil, fmt.Errorf("redis: scan get %s: %w", k, err)
		}
		out = append(out, core.KeyValue{Key: k, Value: []byte(v)})
	}
	return out, nil
}

func (p *Provider) Expire(ctx context.Context, key string, ttl int64) error {
	if err := p.check(); err != nil {
		return err
	}
	key = p.pfx.kv + key
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	if err := p.rd.Expire(ctx, key, secondsDuration(ttl)).Err(); err != nil {
		return fmt.Errorf("redis: expire: %w", err)
	}
	return nil
}

func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	if err := p.check(); err != nil {
		return 0, false, err
	}
	key = p.pfx.kv + key
	d, err := p.rd.TTL(ctx, key).Result()
	if err != nil {
		return 0, false, fmt.Errorf("redis: ttl: %w", err)
	}
	// Redis：-2 key 不存在，-1 无 TTL；统一映射 ok=false。
	if d < 0 {
		return -1, false, nil
	}
	return int64(d / time.Second), true, nil
}

// ---- Queue（Redis List 映射：队尾=右，队头=左） ----

func (p *Provider) QPush(ctx context.Context, name string, value []byte) error {
	if err := p.check(); err != nil {
		return err
	}
	name = p.pfx.q + name
	if err := p.rd.RPush(ctx, name, string(value)).Err(); err != nil {
		return fmt.Errorf("redis: rpush: %w", err)
	}
	return nil
}

func (p *Provider) QPushFront(ctx context.Context, name string, value []byte) error {
	if err := p.check(); err != nil {
		return err
	}
	name = p.pfx.q + name
	if err := p.rd.LPush(ctx, name, string(value)).Err(); err != nil {
		return fmt.Errorf("redis: lpush: %w", err)
	}
	return nil
}

func (p *Provider) QPop(ctx context.Context, name string) ([]byte, bool, error) {
	if err := p.check(); err != nil {
		return nil, false, err
	}
	name = p.pfx.q + name
	v, err := p.rd.LPop(ctx, name).Result()
	if errors.Is(err, goredis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redis: lpop: %w", err)
	}
	return []byte(v), true, nil
}

func (p *Provider) QPopBack(ctx context.Context, name string) ([]byte, bool, error) {
	if err := p.check(); err != nil {
		return nil, false, err
	}
	name = p.pfx.q + name
	v, err := p.rd.RPop(ctx, name).Result()
	if errors.Is(err, goredis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redis: rpop: %w", err)
	}
	return []byte(v), true, nil
}

func (p *Provider) QSize(ctx context.Context, name string) (int64, error) {
	if err := p.check(); err != nil {
		return 0, err
	}
	name = p.pfx.q + name
	n, err := p.rd.LLen(ctx, name).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: llen: %w", err)
	}
	return n, nil
}

func (p *Provider) QFront(ctx context.Context, name string) ([]byte, bool, error) {
	return p.lindex(ctx, name, 0)
}

func (p *Provider) QBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.lindex(ctx, name, -1)
}

func (p *Provider) lindex(ctx context.Context, name string, idx int64) ([]byte, bool, error) {
	if err := p.check(); err != nil {
		return nil, false, err
	}
	name = p.pfx.q + name
	v, err := p.rd.LIndex(ctx, name, idx).Result()
	if errors.Is(err, goredis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redis: lindex: %w", err)
	}
	return []byte(v), true, nil
}

// ---- ZSet（Redis Sorted Set） ----

func (p *Provider) ZSet(ctx context.Context, name, key string, score int64) error {
	if err := p.check(); err != nil {
		return err
	}
	name = p.pfx.z + name
	if err := p.rd.ZAdd(ctx, name, goredis.Z{Score: f64(score), Member: key}).Err(); err != nil {
		return fmt.Errorf("redis: zadd: %w", err)
	}
	return nil
}

func (p *Provider) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	if err := p.check(); err != nil {
		return 0, false, err
	}
	name = p.pfx.z + name
	s, err := p.rd.ZScore(ctx, name, key).Result()
	if errors.Is(err, goredis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("redis: zscore: %w", err)
	}
	return i64(s), true, nil
}

func (p *Provider) ZDel(ctx context.Context, name, key string) error {
	if err := p.check(); err != nil {
		return err
	}
	name = p.pfx.z + name
	if err := p.rd.ZRem(ctx, name, key).Err(); err != nil {
		return fmt.Errorf("redis: zrem: %w", err)
	}
	return nil
}

func (p *Provider) ZSize(ctx context.Context, name string) (int64, error) {
	if err := p.check(); err != nil {
		return 0, err
	}
	name = p.pfx.z + name
	n, err := p.rd.ZCard(ctx, name).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: zcard: %w", err)
	}
	return n, nil
}

func (p *Provider) ZRank(ctx context.Context, name, key string) (int64, bool, error) {
	if err := p.check(); err != nil {
		return 0, false, err
	}
	name = p.pfx.z + name
	r, err := p.rd.ZRank(ctx, name, key).Result()
	if errors.Is(err, goredis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("redis: zrank: %w", err)
	}
	return r, true, nil
}

func (p *Provider) ZRange(ctx context.Context, name string, start, stop int64) ([]core.ZItem, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	name = p.pfx.z + name
	zs, err := p.rd.ZRangeWithScores(ctx, name, start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: zrange: %w", err)
	}
	out := make([]core.ZItem, 0, len(zs))
	for _, z := range zs {
		member, ok := z.Member.(string)
		if !ok {
			continue
		}
		out = append(out, core.ZItem{Key: member, Score: i64(z.Score)})
	}
	return out, nil
}

func (p *Provider) ZIncr(ctx context.Context, name, key string, delta int64) (int64, error) {
	if err := p.check(); err != nil {
		return 0, err
	}
	name = p.pfx.z + name
	s, err := p.rd.ZIncrBy(ctx, name, f64(delta), key).Result()
	if err != nil {
		if notInteger(err) {
			return 0, core.ErrNotInteger
		}
		return 0, fmt.Errorf("redis: zincrby: %w", err)
	}
	return i64(s), nil
}

// ---- 工具 ----

// f64/i64 在 int64 与 float64 间换算。|v|<=2^53 时精确；超出部分 README 已说明。
func f64(v int64) float64 { return float64(v) }
func i64(f float64) int64 { return int64(f) }

// secondsDuration 把秒数换算为 time.Duration。time.Duration 是 int64 纳秒，
// ttl > MaxInt64/1e9（约 292 年）时直接换算会回绕成负数：EXPIRE 收到负值会
// **立即删除键**，SET 收到负值会静默跳过 EX——这里钳制到上限，语义是
// "足够长"，而非毁数据。
func secondsDuration(ttl int64) time.Duration {
	const maxSeconds = math.MaxInt64 / int64(time.Second)
	if ttl > maxSeconds {
		ttl = maxSeconds
	}
	return time.Duration(ttl) * time.Second
}

// notInteger 识别 Redis "value is not an integer or out of range" 类错误。
func notInteger(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "not an integer")
}

// ScanCount 是每轮 Redis SCAN 的 COUNT 提示值（服务端仅按它决定单轮工作量）。
var ScanCount = 256

// normalizeLimit 保证 limit<=0 时使用 core.DefaultScanLimit（统一常量）。
func normalizeLimit(limit int) int {
	if limit <= 0 {
		return core.DefaultScanLimit
	}
	return limit
}

// ---- Batch ----

// ApplyBatch 实现 core.BatchProvider：整批命令用一次 MULTI/EXEC（TxPipeline）
// 发出，N 次往返压缩为 1 次。Redis 的 EXEC 不因单条命令运行时错误回滚
// （命令级语义），但本基座的批操作均为无条件写，正常路径下不会失败；
// 网络/协议错误则整批不返回成功，调用方可按失败处理。
func (p *Provider) ApplyBatch(ctx context.Context, ops []core.BatchOp) error {
	if err := p.check(); err != nil {
		return err
	}
	if len(ops) == 0 {
		return nil
	}
	for _, op := range ops {
		switch op.Kind {
		case core.BatchSetEx, core.BatchExpire:
			if op.TTL <= 0 {
				return core.ErrInvalidTTL
			}
		}
	}

	_, err := p.rd.TxPipelined(ctx, func(pipe goredis.Pipeliner) error {
		for _, op := range ops {
			switch op.Kind {
			case core.BatchSet:
				// KEEPTTL：与单条 Set 一致，批内 Set 不得清掉既有 TTL
				//（裸 SET 会清 TTL，违反 batch 契约；需 Redis >= 6.0）。
				if err := pipe.SetArgs(ctx, p.pfx.kv+op.Key, op.Value, goredis.SetArgs{KeepTTL: true}).Err(); err != nil {
					return err
				}
			case core.BatchSetEx:
				if err := pipe.Set(ctx, p.pfx.kv+op.Key, op.Value, secondsDuration(op.TTL)).Err(); err != nil {
					return err
				}
			case core.BatchDel:
				if err := pipe.Del(ctx, p.pfx.kv+op.Key).Err(); err != nil {
					return err
				}
			case core.BatchExpire:
				if err := pipe.Expire(ctx, p.pfx.kv+op.Key, secondsDuration(op.TTL)).Err(); err != nil {
					return err
				}
			case core.BatchQPush:
				if err := pipe.RPush(ctx, p.pfx.q+op.Key, op.Value).Err(); err != nil {
					return err
				}
			case core.BatchQPushFront:
				if err := pipe.LPush(ctx, p.pfx.q+op.Key, op.Value).Err(); err != nil {
					return err
				}
			case core.BatchZSet:
				if err := pipe.ZAdd(ctx, p.pfx.z+op.Key, goredis.Z{Score: f64(op.Score), Member: op.Member}).Err(); err != nil {
					return err
				}
			case core.BatchZDel:
				if err := pipe.ZRem(ctx, p.pfx.z+op.Key, op.Member).Err(); err != nil {
					return err
				}
			case core.BatchZIncr:
				if err := pipe.ZIncrBy(ctx, p.pfx.z+op.Key, f64(op.Delta), op.Member).Err(); err != nil {
					return err
				}
			default:
				return fmt.Errorf("redis: unknown batch op %d", op.Kind)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("redis: batch: %w", err)
	}
	return nil
}
