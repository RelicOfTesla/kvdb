//go:build go1.27

package kvdb_test

import (
	"context"
	"errors"
	"testing"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

type tUser struct {
	ID   int64    `json:"id"`
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

// TestTypedGet 验证薄壳的核心形态：db.Get[T](...)（Go 1.27 泛型方法）。
func TestTypedGet(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tdb := kvdb.Typed(db)

	// 结构体（默认 JSON）
	want := tUser{ID: 7, Name: "alice", Tags: []string{"a", "b"}}
	if err := db.Set(ctx, "u:7", kvdb.B(want)); err != nil {
		t.Fatal(err)
	}
	got, err := tdb.Get[tUser](ctx, "u:7")
	if err != nil {
		t.Fatalf("Get[tUser]: %v", err)
	}
	if got.ID != want.ID || got.Name != want.Name || len(got.Tags) != 2 {
		t.Fatalf("结构体不一致: %+v", got)
	}

	// 标量：与 Incr 互操作
	if _, err := db.Incr(ctx, "visits", 5); err != nil {
		t.Fatal(err)
	}
	n, err := tdb.Get[int64](ctx, "visits")
	if err != nil || n != 5 {
		t.Fatalf("Get[int64] = %d, %v", n, err)
	}
	if _, err := db.Incr(ctx, "visits", 2); err != nil {
		t.Fatal(err)
	}
	if n, err := tdb.Get[int64](ctx, "visits"); err != nil || n != 7 {
		t.Fatalf("Incr 后 Get[int64] = %d, %v", n, err)
	}

	// 缺失 -> ErrNotFound
	if _, err := tdb.Get[tUser](ctx, "missing"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("缺失应 ErrNotFound, got %v", err)
	}

	// GetOK 保留 ok 语义
	if v, ok, err := tdb.GetOK[string](ctx, "missing"); err != nil || ok || v != "" {
		t.Fatalf("GetOK 缺失 = %q,%v,%v", v, ok, err)
	}
	if v, ok, err := tdb.GetOK[int64](ctx, "visits"); err != nil || !ok || v != 7 {
		t.Fatalf("GetOK = %d,%v,%v", v, ok, err)
	}

	// MGet 批量解码（结构体 + 标量混合场景）
	db.Set(ctx, "u:8", kvdb.B(tUser{ID: 8, Name: "bob"}))
	ms, err := tdb.MGet[tUser](ctx, "u:7", "u:8", "missing")
	if err != nil || len(ms) != 2 || ms["u:8"].Name != "bob" {
		t.Fatalf("MGet[tUser] = %+v, %v", ms, err)
	}
}

// TestTypedQueue 验证队列读取的泛型形态。
func TestTypedQueue(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tdb := kvdb.Typed(db)

	// 元素为结构体
	j1 := tUser{ID: 1, Name: "job-1"}
	j2 := tUser{ID: 2, Name: "job-2"}
	db.QPush(ctx, "jobs", kvdb.B(j1))
	db.QPush(ctx, "jobs", kvdb.B(j2))

	if got, err := tdb.QFront[tUser](ctx, "jobs"); err != nil || got.Name != "job-1" {
		t.Fatalf("QFront = %+v, %v", got, err)
	}
	if got, err := tdb.QBack[tUser](ctx, "jobs"); err != nil || got.Name != "job-2" {
		t.Fatalf("QBack = %+v, %v", got, err)
	}
	if got, err := tdb.QPop[tUser](ctx, "jobs"); err != nil || got.ID != 1 {
		t.Fatalf("QPop = %+v, %v", got, err)
	}
	if got, err := tdb.QPopBack[tUser](ctx, "jobs"); err != nil || got.ID != 2 {
		t.Fatalf("QPopBack = %+v, %v", got, err)
	}
	// 空队列 -> ErrNotFound
	if _, err := tdb.QPop[tUser](ctx, "jobs"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("空队列应 ErrNotFound, got %v", err)
	}
}

// TestTypedPassthrough 验证薄壳未遮蔽的方法仍透传（Set/QPush/Batch/Close）。
func TestTypedPassthrough(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tdb := kvdb.Typed(db)

	// 透传内嵌 DB 的写方法
	if err := tdb.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("透传 Set: %v", err)
	}
	if err := tdb.QPush(ctx, "q", []byte("x")); err != nil {
		t.Fatalf("透传 QPush: %v", err)
	}
	if err := tdb.Batch(ctx, func(b *kvdb.Batch) error {
		b.Set("b1", []byte("v1"))
		return nil
	}); err != nil {
		t.Fatalf("透传 Batch: %v", err)
	}
	if v, err := tdb.Get[string](ctx, "b1"); err != nil || v != "v1" {
		t.Fatalf("Batch 写入后 Get = %q, %v", v, err)
	}

	// 泛型方法遮蔽后 TypedDB 不再满足 DB，但可通过内嵌字段取回
	var asDB kvdb.DB = tdb.DB
	if v, ok, err := asDB.Get(ctx, "k"); err != nil || !ok || string(v) != "v" {
		t.Fatalf("底层 DB Get = %q,%v,%v", v, ok, err)
	}
	if err := tdb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
