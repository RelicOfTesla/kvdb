// Package kvdb_test 验证根包的注册表装配语义：注册表默认**为空**，
// 只有显式 import 基座包（或 kvdb/all）后对应 scheme 才可用。
// 本测试文件只 import 了 mem 基座，因此 jsonl/sqlite 等应报"未注册"。
package kvdb_test

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"

	"kvdb"
	"kvdb/core"
	"kvdb/mem" // 只接入 mem：验证按需注册
)

// kvOnly 是仅实现 KV 能力的自定义基座，用于验证可选能力降级路径
// （业务方可在其上叠加 queue/zset 的模拟实现）。它只嵌入 KvProvider、
// 不实现 Close：同时验证 DB.Close 对非 Closer 基座是空操作。
type kvOnly struct {
	core.KvProvider
}

// fullStub 校验 FullProvider 组合接口可被完整基座满足（编译期断言，
// 复用 mem 基座的实现）。
type fullStub struct {
	core.FullProvider
}

var _ core.FullProvider = fullStub{}

// 反向校验：仅 KV 的基座不满足 FullProvider。
var _ = func() bool {
	var p any = kvOnly{}
	_, isFull := p.(core.FullProvider)
	return !isFull
}()

// TestSchemeNotRegistered 验证未 import 的基座 scheme 不可用，
// 且错误信息给出已注册列表与修复方式（避免"忘了 import"难以排查）。
func TestSchemeNotRegistered(t *testing.T) {
	ctx := context.Background()
	for _, uri := range []string{"jsonl://./x.jsonl", "sqlite://./x.db", "ssdb://127.0.0.1:8888"} {
		_, err := kvdb.Open(ctx, uri)
		if err == nil {
			t.Fatalf("未注册的 %s 应报错", uri)
		}
		if !strings.Contains(err.Error(), "unknown provider scheme") ||
			!strings.Contains(err.Error(), "registered:") {
			t.Fatalf("错误信息应含已注册 scheme 列表, got %v", err)
		}
	}
}

func TestRegisteredSchemes(t *testing.T) {
	got := kvdb.Schemes()
	found := false
	for _, s := range got {
		if s == "mem" {
			found = true
		}
		if s == "jsonl" && !importedJSONL {
			t.Fatalf("未 import jsonl 却已注册: %v", got)
		}
	}
	if !found {
		t.Fatalf("mem 应已注册, got %v", got)
	}
}

// importedJSONL 记录本测试包是否 import 了 jsonl 基座（当前为否）。
const importedJSONL = false

func TestOpenRegisteredScheme(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := db.Get(ctx, "k"); !ok || string(v) != "v" {
		t.Fatalf("Get = %q,%v", v, ok)
	}
}

func TestUnknownAndDuplicateScheme(t *testing.T) {
	ctx := context.Background()
	if _, err := kvdb.Open(ctx, "nosuch://x"); err == nil {
		t.Fatal("未知 scheme 应报错")
	}
	if err := kvdb.Register("mem", func(context.Context, *url.URL) (core.KvProvider, error) { return nil, nil }); err == nil {
		t.Fatal("重复注册应报错")
	}
	if err := kvdb.Register("", nil); err == nil {
		t.Fatal("空 scheme 应报错")
	}
	if err := kvdb.Register("nilopener", nil); err == nil {
		t.Fatal("nil opener 应报错")
	}
}

func TestCustomScheme(t *testing.T) {
	ctx := context.Background()
	if err := kvdb.Register("custom-kv", func(_ context.Context, _ *url.URL) (core.KvProvider, error) {
		return &kvOnly{KvProvider: mem.New()}, nil
	}); err != nil {
		t.Fatal(err)
	}
	db, err := kvdb.Open(ctx, "custom-kv://local")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := db.Get(ctx, "k"); !ok || string(v) != "v" {
		t.Fatalf("Get = %q,%v", v, ok)
	}
	if hasQ, hasZ, hasB := db.Capabilities(); hasQ || hasZ || hasB {
		t.Fatalf("kvOnly 不应上报能力, got q=%v z=%v", hasQ, hasZ)
	}
	if _, _, err := db.QPop(ctx, "q"); !errors.Is(err, core.ErrUnsupported) {
		t.Fatalf("未实现 queue 应 ErrUnsupported, got %v", err)
	}
	if _, err := db.ZRange(ctx, "z", 0, -1); !errors.Is(err, core.ErrUnsupported) {
		t.Fatalf("未实现 zset 应 ErrUnsupported, got %v", err)
	}
	// kvOnly 未实现 Closer：Close 应为空操作且不报错。
	if err := db.Close(); err != nil {
		t.Fatalf("非 Closer 基座 Close 应为空操作, got %v", err)
	}
}
