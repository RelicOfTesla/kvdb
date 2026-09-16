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

	"github.com/RelicOfTesla/kvdb/core"
)

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

// IncrWraps 能力声明：Redis INCRBY 溢出由服务端报错（映射 ErrNotInteger，不回绕），
// 见 core.IncrWrapsProvider 与 Caps.IncrWraps。
func (p *Provider) IncrWraps() bool { return false }

var _ core.IncrWrapsProvider = (*Provider)(nil)

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
	if err := core.CheckKey(key); err != nil {
		return err
	}
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
	if err := core.CheckKey(key); err != nil {
		return err
	}
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

// SetExAt 写入 value 并让 key 在 at（unix 秒）过期；at 已过去则删除该 key。
//
// Redis 的 SET 没有"带绝对到期时刻"的写法（EXAT 自 6.2 起，但 go-redis 的
// SetArgs 未暴露），因此用 SET + EXPIREAT 两条命令、放在一次 pipeline 里发出，
// 保证两者之间不会被其他客户端的写入插进来。注意这仍是**两条命令**：
// Redis 的 MULTI/EXEC 不做命令级回滚，若 EXPIREAT 失败，SET 的结果会保留
// （与 Batch 的既有说明一致）。
func (p *Provider) SetExAt(ctx context.Context, key string, value []byte, at int64) error {
	if err := core.CheckKey(key); err != nil {
		return err
	}
	if err := p.check(); err != nil {
		return err
	}
	key = p.pfx.kv + key
	if at <= core.NowUnix() {
		if err := p.rd.Del(ctx, key).Err(); err != nil {
			return fmt.Errorf("redis: setexat: %w", err)
		}
		return nil
	}
	pipe := p.rd.TxPipeline()
	pipe.Set(ctx, key, string(value), 0)
	pipe.ExpireAt(ctx, key, time.Unix(at, 0))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis: setexat: %w", err)
	}
	return nil
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := core.CheckKey(key); err != nil {
		return nil, false, err
	}
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
	if err := core.CheckKey(key); err != nil {
		return err
	}
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
	if err := core.CheckKey(key); err != nil {
		return false, err
	}
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
	if err := core.CheckKey(key); err != nil {
		return 0, err
	}
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
	if err := core.CheckKey(key); err != nil {
		return err
	}
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

// ExpireAt 让 key 在 at（unix 秒）过期；at 已过去则立即删除。
// 直接用 Redis 原生 EXPIREAT：过去时间点由服务端立即删除该 key。
func (p *Provider) ExpireAt(ctx context.Context, key string, at int64) error {
	if err := core.CheckKey(key); err != nil {
		return err
	}
	if err := p.check(); err != nil {
		return err
	}
	if err := p.rd.ExpireAt(ctx, p.pfx.kv+key, time.Unix(at, 0)).Err(); err != nil {
		return fmt.Errorf("redis: expireat: %w", err)
	}
	return nil
}

func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	if err := core.CheckKey(key); err != nil {
		return 0, false, err
	}
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
	if err := core.CheckKey(name); err != nil {
		return err
	}
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
	if err := core.CheckKey(name); err != nil {
		return err
	}
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
	if err := core.CheckKey(name); err != nil {
		return nil, false, err
	}
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
	if err := core.CheckKey(name); err != nil {
		return nil, false, err
	}
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
	if err := core.CheckKey(name); err != nil {
		return 0, err
	}
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

// QRange 只读返回 [start, stop] 索引区间内的元素，方向为队头 → 队尾。
//
// 直接用 Redis LRANGE：其语义（0 起闭区间、负索引从末尾数、越界按可用范围裁剪、
// 队列不存在视为空、返回顺序恰好是队头 → 队尾）与本契约完全一致，无需客户端换算。
// 队列为空（或 key 不存在）时 LRANGE 返回空数组而非 nil 错误，故直接映射为空切片。
func (p *Provider) QRange(ctx context.Context, name string, start, stop int64) ([][]byte, error) {
	if err := core.CheckKey(name); err != nil {
		return nil, err
	}
	if err := p.check(); err != nil {
		return nil, err
	}
	name = p.pfx.q + name
	vals, err := p.rd.LRange(ctx, name, start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: qrange: %w", err)
	}
	return stringsToBytes(vals), nil
}

func (p *Provider) lindex(ctx context.Context, name string, idx int64) ([]byte, bool, error) {
	if err := core.CheckKey(name); err != nil {
		return nil, false, err
	}
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
	if err := core.CheckKeys(name, key); err != nil {
		return err
	}
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
	if err := core.CheckKeys(name, key); err != nil {
		return 0, false, err
	}
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
	if err := core.CheckKeys(name, key); err != nil {
		return err
	}
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
	if err := core.CheckKey(name); err != nil {
		return 0, err
	}
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
	if err := core.CheckKeys(name, key); err != nil {
		return 0, false, err
	}
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
	if err := core.CheckKey(name); err != nil {
		return nil, err
	}
	if err := p.check(); err != nil {
		return nil, err
	}
	name = p.pfx.z + name
	zs, err := p.rd.ZRangeWithScores(ctx, name, start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: zrange: %w", err)
	}
	return zitems(zs), nil
}

// ZRangeByScore 返回分数落在闭区间 [min, max] 内的成员。
//
// desc 只改遍历方向，不交换 min/max：go-redis 的 ZRevRangeByScoreWithScores
// 对外虽以 (&ZRangeBy{Max: ..., Min: ...}) 命名参数，但其内部把它拼成
// `ZREVRANGEBYSCORE key <Max> <Min>`（Max 在前），所以这里传 Max=max、Min=min，
// 命令实际发出 `ZREVRANGEBYSCORE key max min`，区间仍是 [min, max]——参数含义
// 与升序分支完全一致。
//
// 同分成员的次序：Redis 的 ZREVRANGEBYSCORE 在同分时按成员**降序**返回（升序命令
// ZRANGEBYSCORE 才是成员升序），因此 desc 分支需要对每个同分组做一次反转，
// 才能满足"desc 时同分成员仍按成员字节序升序"的契约（见 core.ZSetProvider 注释）。
// 顺带一提：升序分支的 [min, max] 闭区间由 Redis 的 "min<=score<=max" 语义天然满足。
func (p *Provider) ZRangeByScore(ctx context.Context, name string, min, max int64, limit int, desc bool) ([]core.ZItem, error) {
	if err := core.CheckKey(name); err != nil {
		return nil, err
	}
	if err := p.check(); err != nil {
		return nil, err
	}
	// min > max 是空区间：直接返回，不打扰服务端（与 mem 参考实现一致）。
	if min > max {
		return nil, nil
	}
	name = p.pfx.z + name
	// 分数是 float64，本 SDK 契约是 int64：必须按整数格式化，否则大整数的
	// 十进制往返会因浮点表示不精确而落到区间之外（闭区间边界要求精确）。
	by := &goredis.ZRangeBy{
		Min: strconv.FormatInt(min, 10),
		Max: strconv.FormatInt(max, 10),
	}
	if limit > 0 {
		by.Offset = 0
		by.Count = int64(limit)
	}
	var (
		zs  []goredis.Z
		err error
	)
	if desc {
		zs, err = p.rd.ZRevRangeByScoreWithScores(ctx, name, by).Result()
	} else {
		zs, err = p.rd.ZRangeByScoreWithScores(ctx, name, by).Result()
	}
	if err != nil {
		return nil, fmt.Errorf("redis: zrangebyscore: %w", err)
	}
	out := zitems(zs)
	if desc {
		fixDescTieOrder(out)
	}
	return out, nil
}

// zitems 把 go-redis 的 []Z 转成 []core.ZItem（成员非 string 时跳过）。
func zitems(zs []goredis.Z) []core.ZItem {
	out := make([]core.ZItem, 0, len(zs))
	for _, z := range zs {
		member, ok := z.Member.(string)
		if !ok {
			continue
		}
		out = append(out, core.ZItem{Key: member, Score: i64(z.Score)})
	}
	return out
}

// fixDescTieOrder 就地反转 out 中每个**同分连续段**。ZREVRANGEBYSCORE 返回的是
// 分数降序、同分成员降序；逐段反转后同分成员回到升序，段间仍是分数降序。
func fixDescTieOrder(out []core.ZItem) {
	for i := 0; i < len(out); {
		j := i + 1
		for j < len(out) && out[j].Score == out[i].Score {
			j++
		}
		for l, r := i, j-1; l < r; l, r = l+1, r-1 {
			out[l], out[r] = out[r], out[l]
		}
		i = j
	}
}

func (p *Provider) ZIncr(ctx context.Context, name, key string, delta int64) (int64, error) {
	if err := core.CheckKeys(name, key); err != nil {
		return 0, err
	}
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

// stringsToBytes 把 go-redis 返回的 []string 转为 [][]byte。
// []byte(s) 是逐字节复制（不是 UTF-8 重新编码），任意二进制值原样往返。
// nil 入参返回 nil，与"空队列返回空切片"的契约一致。
func stringsToBytes(ss []string) [][]byte {
	if ss == nil {
		return nil
	}
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

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
		// 空 key/队列名/zset 名（以及 zset 成员）整批拒绝：与单条路径同口径，
		// 且**不得静默跳过**（跳过会让调用方以为整批已写入，见 core.ErrInvalidKey）。
		switch op.Kind {
		case core.BatchZSet, core.BatchZDel, core.BatchZIncr:
			if err := core.CheckKeys(op.Key, op.Member); err != nil {
				return err
			}
		default:
			if err := core.CheckKey(op.Key); err != nil {
				return err
			}
		}
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
