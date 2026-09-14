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
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"kvdb"
	"kvdb/core"
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

// Config 是 Redis 连接配置。
type Config struct {
	Addr     string // host:port
	Username string
	Password string
	DB       int // 库号（0-15）
	PoolSize int // 0 使用驱动默认值
}

// Provider 是 Redis 基座。go-redis 内部自带连接池，并发安全。
type Provider struct {
	rd     *goredis.Client
	closed bool
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
	return &Provider{rd: rd}, nil
}

func (p *Provider) check() error {
	if p.closed {
		return core.ErrClosed
	}
	return nil
}

func (p *Provider) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	return p.rd.Close()
}

// ---- KV ----

func (p *Provider) Set(ctx context.Context, key string, value []byte) error {
	if err := p.check(); err != nil {
		return err
	}
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
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	if err := p.rd.Set(ctx, key, string(value), time.Duration(ttl)*time.Second).Err(); err != nil {
		return fmt.Errorf("redis: setex: %w", err)
	}
	return nil
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := p.check(); err != nil {
		return nil, false, err
	}
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
	if err := p.rd.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis: del: %w", err)
	}
	return nil
}

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	if err := p.check(); err != nil {
		return false, err
	}
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
	vals, err := p.rd.MGet(ctx, keys...).Result()
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

	// 全量游标扫描收集命中区间的 key（SCAN 无序，无法提前截断），客户端排序。
	var found []string
	cur := uint64(0)
	for {
		keys, next, err := p.rd.Scan(ctx, cur, "*", 256).Result()
		if err != nil {
			return nil, fmt.Errorf("redis: scan: %w", err)
		}
		for _, k := range keys {
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
		v, err := p.rd.Get(ctx, k).Result()
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
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	if err := p.rd.Expire(ctx, key, time.Duration(ttl)*time.Second).Err(); err != nil {
		return fmt.Errorf("redis: expire: %w", err)
	}
	return nil
}

func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	if err := p.check(); err != nil {
		return 0, false, err
	}
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
	if err := p.rd.RPush(ctx, name, string(value)).Err(); err != nil {
		return fmt.Errorf("redis: rpush: %w", err)
	}
	return nil
}

func (p *Provider) QPushFront(ctx context.Context, name string, value []byte) error {
	if err := p.check(); err != nil {
		return err
	}
	if err := p.rd.LPush(ctx, name, string(value)).Err(); err != nil {
		return fmt.Errorf("redis: lpush: %w", err)
	}
	return nil
}

func (p *Provider) QPop(ctx context.Context, name string) ([]byte, bool, error) {
	if err := p.check(); err != nil {
		return nil, false, err
	}
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
	if err := p.rd.ZAdd(ctx, name, goredis.Z{Score: f64(score), Member: key}).Err(); err != nil {
		return fmt.Errorf("redis: zadd: %w", err)
	}
	return nil
}

func (p *Provider) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	if err := p.check(); err != nil {
		return 0, false, err
	}
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
	if err := p.rd.ZRem(ctx, name, key).Err(); err != nil {
		return fmt.Errorf("redis: zrem: %w", err)
	}
	return nil
}

func (p *Provider) ZSize(ctx context.Context, name string) (int64, error) {
	if err := p.check(); err != nil {
		return 0, err
	}
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

// notInteger 识别 Redis "value is not an integer or out of range" 类错误。
func notInteger(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "not an integer")
}

func normalizeLimit(limit int) int {
	const defaultLimit = 100 // 与 core.DefaultScanLimit 对齐
	if limit <= 0 {
		return defaultLimit
	}
	return limit
}
