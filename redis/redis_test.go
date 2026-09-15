package redis_test

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/kvdbtest"
	"github.com/RelicOfTesla/kvdb/redis"
)

// TestBehavior 用 miniredis（进程内 Redis 替身）验证基座行为。
// miniredis 不随真实时间过期，需通过 FastForward 推进虚拟时钟；
// 且整个实例在所有子用例间共享，故每次新建基座前清空。
func TestBehavior(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)

	kvdbtest.RunWithOptions(t, kvdbtest.Options{
		FastForward: func() { s.FastForward(2 * time.Second) },
	}, func(t *testing.T) core.KvProvider {
		s.FlushAll()
		p, err := redis.Open(t.Context(), redis.Config{Addr: s.Addr()})
		if err != nil {
			t.Fatal(err)
		}
		return p
	})
}

// TestRealRedis 仅当显式设置 KVDB_TEST_REDIS_ADDR（如 127.0.0.1:6379）时
// 对真实 Redis 跑行为用例，默认跳过（本机服务类测试按仓库约定走环境变量开关）。
// 使用独立的库号（默认 15）并在开始前清空，避免污染同实例的其他数据。
func TestRealRedis(t *testing.T) {
	addr := os.Getenv("KVDB_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("未设置 KVDB_TEST_REDIS_ADDR，跳过真实 Redis 测试")
	}
	db := 15
	if v := os.Getenv("KVDB_TEST_REDIS_DB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			db = n
		}
	}
	kvdbtest.Run(t, func(t *testing.T) core.KvProvider {
		p, err := redis.Open(t.Context(), redis.Config{Addr: addr, DB: db})
		if err != nil {
			t.Fatal(err)
		}
		// 直接经驱动清空该库：真实实例可能残留上次运行的数据，
		// 而用例断言基于"仅本用例写入"的前提（不给生产代码加测试专用方法）。
		cli := goredis.NewClient(&goredis.Options{Addr: addr, DB: db})
		if err := cli.FlushDB(t.Context()).Err(); err != nil {
			t.Fatal(err)
		}
		cli.Close()
		return p
	})
}

// TestMiniredisScan 验证 SCAN 语义（KEYSPACE 范围过滤 + 客户端排序）。
func TestMiniredisScan(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	p, err := redis.Open(t.Context(), redis.Config{Addr: s.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, k := range []string{"scan_a", "scan_b", "scan_c", "zzz"} {
		if err := p.Set(t.Context(), k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	got, err := p.Scan(t.Context(), "scan_a", "scan_c", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Key != "scan_a" || got[2].Key != "scan_c" {
		t.Fatalf("Scan = %v", got)
	}
}
