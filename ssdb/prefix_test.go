package ssdb_test

import (
	"context"
	"testing"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/ssdb"
)

// TestKeyPrefix 验证 Config.KeyPrefix：物理键带前缀、不同前缀互不可见、
// Scan/MGet 返回的是剥掉前缀后的用户键。
func TestKeyPrefix(t *testing.T) {
	ctx := context.Background()
	srv := newFakeSSDB(t)

	a, err := ssdb.OpenWithConfig(ctx, ssdb.Config{Addr: srv.addr(), KeyPrefix: "appA:"})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := ssdb.OpenWithConfig(ctx, ssdb.Config{Addr: srv.addr(), KeyPrefix: "appB:"})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	for _, p := range []*ssdb.Provider{a, b} {
		if err := p.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := p.QPush(ctx, "q", []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := p.ZSet(ctx, "z", "m", 5); err != nil {
			t.Fatal(err)
		}
	}

	// 2) 物理键带前缀（直接查服务端内部存储）
	srv.mu.Lock()
	_, hasA := srv.kv["appA:k"]
	_, hasB := srv.kv["appB:k"]
	_, bare := srv.kv["k"]
	srv.mu.Unlock()
	if !hasA || !hasB || bare {
		t.Fatalf("物理键应为 appA:k / appB:k，实际 hasA=%v hasB=%v bare=%v", hasA, hasB, bare)
	}

	// 1) 互相不可见
	if _, ok, _ := a.Get(ctx, "k"); !ok {
		t.Fatal("本前缀内应可读")
	}
	if err := a.Del(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := a.Get(ctx, "k"); ok {
		t.Fatal("Del 后本命名空间内不应存在")
	}
	if _, ok, _ := b.Get(ctx, "k"); !ok {
		t.Fatal("不同前缀应互不可见（删除 appA 不应影响 appB）")
	}

	// 3) Scan 只覆盖自己的命名空间，且返回剥掉前缀的键
	if err := a.Set(ctx, "k2", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	kvs, err := a.Scan(ctx, "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, kv := range kvs {
		got[kv.Key] = true
	}
	if !got["k2"] {
		t.Fatalf("Scan 应含自身键（已剥前缀），实际 %+v", kvs)
	}
	// appB 独有键不得出现
	if err := b.Set(ctx, "bonly", []byte("1")); err != nil {
		t.Fatal(err)
	}
	kvs2, err := a.Scan(ctx, "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range kvs2 {
		if kv.Key == "bonly" {
			t.Fatalf("Scan 越界读到其他命名空间的键: %+v", kvs2)
		}
	}

	// 4) MGet 同样剥前缀
	m, err := a.MGet(ctx, "k2", "nope")
	if err != nil {
		t.Fatal(err)
	}
	if string(m["k2"]) != "v2" {
		t.Fatalf("MGet 键名未剥前缀: %+v", m)
	}

	// 5) 批写路径也要加前缀
	if err := a.ApplyBatch(ctx, []core.BatchOp{
		{Kind: core.BatchSet, Key: "bk", Value: []byte("bv")},
		{Kind: core.BatchQPush, Key: "bq", Value: []byte("x")},
	}); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := a.Get(ctx, "bk"); !ok || string(v) != "bv" {
		t.Fatalf("批写后读不到: %q,%v", v, ok)
	}
	srv.mu.Lock()
	_, phys := srv.kv["appA:bk"]
	srv.mu.Unlock()
	if !phys {
		t.Fatal("批写的物理键未带前缀")
	}
}
