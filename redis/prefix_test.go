package redis_test

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/RelicOfTesla/kvdb/redis"
)

// TestKeyPrefix 验证 Config.KeyPrefix 生效：三段命名空间各自带前缀，
// 且同一实例上不同前缀的两个基座互不可见（多应用共用一个 Redis 的隔离手段）。
func TestKeyPrefix(t *testing.T) {
	ctx := context.Background()
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)

	a, err := redis.Open(ctx, redis.Config{Addr: s.Addr(), KeyPrefix: "appA:"})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := redis.Open(ctx, redis.Config{Addr: s.Addr(), KeyPrefix: "appB:"})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	for _, p := range []*redis.Provider{a, b} {
		if err := p.Set(ctx, "k", []byte("mine")); err != nil {
			t.Fatal(err)
		}
		if err := p.QPush(ctx, "k", []byte("q")); err != nil { // 同名 KV/队列/zset 不串扰
			t.Fatal(err)
		}
		if err := p.ZSet(ctx, "k", "m", 1); err != nil {
			t.Fatal(err)
		}
	}
	// 互相不可见
	if v, ok, _ := a.Get(ctx, "k"); !ok || string(v) != "mine" {
		t.Fatalf("本前缀内应可读: %q,%v", v, ok)
	}
	// Scan 只返回自己前缀下的 KV 键，且不泄露前缀
	kvs, err := a.Scan(ctx, "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 || kvs[0].Key != "k" {
		t.Fatalf("Scan 结果含前缀或跨库泄漏: %+v", kvs)
	}
	// 物理键确实带前缀
	raw := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	defer raw.Close()
	keys, _, err := raw.Scan(ctx, 0, "*", 100).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"appA:kv:k", "appA:q:k", "appA:z:k", "appB:kv:k"} {
		found := false
		for _, k := range keys {
			if k == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("缺少物理键 %s，实际: %v", want, strings.Join(keys, ","))
		}
	}
}
