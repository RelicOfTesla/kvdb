package kvdb_test

import (
	"context"
	"errors"
	"testing"

	"kvdb"
	"kvdb/core"
)

type myID int64 // 自定义底层类型别名，验证 ~int64 约束

func TestBytesRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		enc  any
	}{
		{"string", "hello"},
		{"int", 42},
		{"int8", int8(-8)},
		{"int64", int64(-1 << 40)},
		{"uint64", uint64(1<<63 + 7)},
		{"float64", 3.141592653589793},
		{"float32", float32(1.5)},
		{"bool-true", true},
		{"bool-false", false},
		{"bytes", []byte{0x00, 0xff, '\n'}},
		{"named-int64", myID(7)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			switch v := c.enc.(type) {
			case string:
				b := kvdb.B(v)
				got, err := kvdb.P[string](b)
				if err != nil || got != v {
					t.Fatalf("P[string](%q) = %q, %v", b, got, err)
				}
			case int:
				b := kvdb.B(v)
				if got, err := kvdb.P[int](b); err != nil || got != v {
					t.Fatalf("P[int] = %d, %v", got, err)
				}
			case int8:
				b := kvdb.B(v)
				if got, err := kvdb.P[int8](b); err != nil || got != v {
					t.Fatalf("P[int8] = %d, %v", got, err)
				}
			case int64:
				b := kvdb.B(v)
				if got, err := kvdb.P[int64](b); err != nil || got != v {
					t.Fatalf("P[int64] = %d, %v", got, err)
				}
			case uint64:
				b := kvdb.B(v)
				if got, err := kvdb.P[uint64](b); err != nil || got != v {
					t.Fatalf("P[uint64] = %d, %v", got, err)
				}
			case float64:
				b := kvdb.B(v)
				if got, err := kvdb.P[float64](b); err != nil || got != v {
					t.Fatalf("P[float64] = %v, %v", got, err)
				}
			case float32:
				b := kvdb.B(v)
				if got, err := kvdb.P[float32](b); err != nil || got != v {
					t.Fatalf("P[float32] = %v, %v", got, err)
				}
			case bool:
				b := kvdb.B(v)
				got, err := kvdb.P[bool](b)
				if err != nil || got != v {
					t.Fatalf("P[bool](%q) = %v, %v", b, got, err)
				}
			case []byte:
				b := kvdb.B(v)
				got, err := kvdb.P[[]byte](b)
				if err != nil || string(got) != string(v) {
					t.Fatalf("P[[]byte] = %q, %v", got, err)
				}
			case myID:
				b := kvdb.B(v)
				if got, err := kvdb.P[myID](b); err != nil || got != v {
					t.Fatalf("P[myID] = %d, %v", got, err)
				}
			}
		})
	}
}

func TestBytesBadParse(t *testing.T) {
	if _, err := kvdb.P[int64]([]byte("abc")); err == nil {
		t.Fatal("非数字解码应报错")
	}
	if _, err := kvdb.P[int8]([]byte("1000")); err == nil {
		t.Fatal("int8 越界应报错")
	}
	if _, err := kvdb.P[bool]([]byte("nope")); err == nil {
		t.Fatal("非法 bool 应报错")
	}
}

// TestDShape 验证用户要求的三种调用形态：
//  1. 写：db.Set(ctx, k, B(x))
//  2. 读：n, err := D[T](db.Get(ctx, k))（Go 多返回值直接展开为实参）
//  3. must：n := DMust[T](db.Get(ctx, k))
func TestDShape(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 形态 1：B 内联写
	if err := db.Set(ctx, "n", kvdb.B(int64(5))); err != nil {
		t.Fatal(err)
	}
	db.Set(ctx, "s", kvdb.B("hello"))
	db.QPush(ctx, "q", kvdb.B(int64(1)))

	// 形态 2：D 合并 Get 三返回值
	n, err := kvdb.D[int64](db.Get(ctx, "n"))
	if err != nil || n != 5 {
		t.Fatalf("D[int64](Get) = %d, %v", n, err)
	}
	s, err := kvdb.D[string](db.Get(ctx, "s"))
	if err != nil || s != "hello" {
		t.Fatalf("D[string](Get) = %q, %v", s, err)
	}
	// QPop 同为 (val, ok, err) 三值，一样适用
	q, err := kvdb.D[int64](db.QPop(ctx, "q"))
	if err != nil || q != 1 {
		t.Fatalf("D[int64](QPop) = %d, %v", q, err)
	}

	// 缺失 → ErrNotFound；与 Incr 互操作
	if _, err := kvdb.D[int64](db.Get(ctx, "missing")); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("缺失 key 应 ErrNotFound, got %v", err)
	}
	if got, err := db.Incr(ctx, "n", 3); err != nil || got != 8 {
		t.Fatalf("Incr 互操作 = %d, %v", got, err)
	}
	if n, err := kvdb.D[int64](db.Get(ctx, "n")); err != nil || n != 8 {
		t.Fatalf("B(int64) 写入后 Incr+读 = %d, %v", n, err)
	}

	// 形态 3：DMust panic
	if v := kvdb.DMust[int64](db.Get(ctx, "n")); v != 8 {
		t.Fatalf("DMust = %d", v)
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("DMust 缺失应 panic")
			} else if !errors.Is(r.(error), core.ErrNotFound) {
				t.Fatalf("panic 应为 ErrNotFound, got %v", r)
			}
		}()
		kvdb.DMust[int64](db.Get(ctx, "missing"))
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("DMust 解析失败应 panic")
			}
		}()
		kvdb.DMust[int64](db.Get(ctx, "s")) // "hello" 非数字
	}()
}
