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

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
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

// 反向校验：仅 KV 的基座不满足 FullProvider（运行期断言，失败即 panic）。
var _ = func() bool {
	var p any = kvOnly{}
	_, isFull := p.(core.FullProvider)
	if isFull {
		panic("kvOnly 不应满足 core.FullProvider")
	}
	return true
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

// 根模块不 import 任何真实基座，因此这里断言：只有测试桩（stub_test.go 注册）
// 在册，而 jsonl/sqlite/mem 等真实基座都必须是"未注册"——一旦根模块被误引入
// 后端依赖，本用例会立刻失败。
func TestRegisteredSchemes(t *testing.T) {
	got := kvdb.Schemes()
	if len(got) != 1 || got[0] != stubScheme {
		t.Fatalf("根模块只应注册测试桩 %q, got %v", stubScheme, got)
	}
	for _, unwanted := range []string{"mem", "jsonl", "sqlite", "bolt", "mysql", "pg", "redis", "ssdb"} {
		for _, s := range got {
			if s == unwanted {
				t.Fatalf("根模块不应注册真实基座 %q（说明引入了后端依赖）: %v", unwanted, got)
			}
		}
	}
}

func TestOpenRegisteredScheme(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "stub://")
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
	if err := kvdb.Register(stubScheme, func(context.Context, *url.URL) (core.KvProvider, error) { return nil, nil }); err == nil {
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
		return &kvOnly{KvProvider: newFakeKV()}, nil
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
	if c := db.Capabilities(); c.Queue || c.ZSet || c.Batch {
		t.Fatalf("kvOnly 不应上报能力, got %+v", c)
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

// TestOpenErrorRedactsCredentials 验证 URI 解析失败的错误信息不会泄露凭据：
// 调用方常直接把 err 写日志，原样带上 userinfo 就等于泄露密码。
func TestOpenErrorRedactsCredentials(t *testing.T) {
	ctx := context.Background()
	const secret = "sup3rs3cr3t-p4ss"
	// 端口非数字 -> url.Parse 失败（注意 Go 不校验端口范围，99999 是合法的），
	// 错误信息里原本会带完整 URI。
	_, err := kvdb.Open(ctx, "mysql://user:"+secret+"@host:notaport/db")
	if err == nil {
		t.Fatal("非法 URI 应报错")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("错误信息泄露了密码: %v", err)
	}
	if !strings.Contains(err.Error(), "mysql://<redacted>") {
		t.Fatalf("错误信息应保留脱敏后的 scheme, got %v", err)
	}
}
