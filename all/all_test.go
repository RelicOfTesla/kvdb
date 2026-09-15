// Package all_test 验证 import _ "github.com/RelicOfTesla/kvdb/all" 后全部内置 scheme 均已注册，
// 且文件型/内存型基座可经 kvdb.Open 真正打开（服务型基座仅校验注册与解析，
// 不在单测中要求外部服务在线）。
package all_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/RelicOfTesla/kvdb"
	_ "github.com/RelicOfTesla/kvdb/all" // 被测对象：一次性注册全部内置基座
)

func TestAllSchemesRegistered(t *testing.T) {
	want := []string{"bolt", "jsonl", "leveldb", "mem", "mysql", "pg", "redis", "sqlite", "ssdb"}
	got := kvdb.Schemes()
	set := make(map[string]bool, len(got))
	for _, s := range got {
		set[s] = true
	}
	for _, w := range want {
		if !set[w] {
			t.Fatalf("scheme %q 未注册，已注册: %v", w, got)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("注册表数量 = %d (%v), want %d", len(got), got, len(want))
	}
}

// TestOpenLocalBackends 经 kvdb.Open 打开本地基座，验证 URI 解析 + 注册链路闭环。
func TestOpenLocalBackends(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	uris := []string{
		"mem://",
		"jsonl://" + filepath.Join(dir, "d.jsonl"),
		"bolt://" + filepath.Join(dir, "d.bolt"),
		"leveldb://" + filepath.Join(dir, "d.ldb"),
		"sqlite://" + filepath.Join(dir, "d.db"),
	}
	for _, uri := range uris {
		db, err := kvdb.Open(ctx, uri)
		if err != nil {
			t.Fatalf("Open(%q): %v", uri, err)
		}
		if err := db.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatalf("Open(%q) Set: %v", uri, err)
		}
		if v, ok, _ := db.Get(ctx, "k"); !ok || string(v) != "v" {
			t.Fatalf("Open(%q) Get = %q,%v", uri, v, ok)
		}
		if hasQ, hasZ, hasB := db.Capabilities(); !hasQ || !hasZ || !hasB {
			t.Fatalf("Open(%q) 应具备 queue/zset 能力, got q=%v z=%v", uri, hasQ, hasZ)
		}
		db.Close()
	}
}
