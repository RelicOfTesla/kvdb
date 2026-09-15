package mem_test

import (
	"context"
	"testing"

	"github.com/RelicOfTesla/kvdb"
	_ "github.com/RelicOfTesla/kvdb/mem" // blank import 触发 mem 的 init 自注册
)

// TestSelfRegistration 验证 mem 模块被 import 时会把自己注册为 "mem"，
// 且经注册表 kvdb.Open 拿到的基座可正常完成三类数据的往返。
//
// "import 即注册"这一语义的覆盖归属于实现模块自身：根模块不 import 任何基座
// （见根模块的 TestRegisteredSchemes），因此不能由根模块来验证它。
func TestSelfRegistration(t *testing.T) {
	ctx := context.Background()

	found := false
	for _, s := range kvdb.Schemes() {
		if s == "mem" {
			found = true
		}
	}
	if !found {
		t.Fatalf("mem 应已自注册, got %v", kvdb.Schemes())
	}

	db, err := kvdb.Open(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := db.Get(ctx, "k"); err != nil || !ok || string(v) != "v" {
		t.Fatalf("Get = %q,%v,%v", v, ok, err)
	}
	if n, err := db.Incr(ctx, "n", 2); err != nil || n != 2 {
		t.Fatalf("Incr = %d,%v", n, err)
	}
	if err := db.QPush(ctx, "q", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := db.QPop(ctx, "q"); !ok || string(v) != "x" {
		t.Fatalf("QPop = %q,%v", v, ok)
	}
	if err := db.ZSet(ctx, "z", "m", 3); err != nil {
		t.Fatal(err)
	}
	if s, ok, _ := db.ZGet(ctx, "z", "m"); !ok || s != 3 {
		t.Fatalf("ZGet = %d,%v", s, ok)
	}
}
