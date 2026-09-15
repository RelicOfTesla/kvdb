package kvdb_test

import (
	"context"
	"errors"
	"testing"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
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
	db, err := kvdb.Open(ctx, "stub://")
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

// ---- 结构体 <-> 字节（默认 JSON 编解码）----

type Address struct {
	City string `json:"city"`
	Zip  string `json:"zip,omitempty"`
}

type User struct {
	ID      int64    `json:"id"`
	Name    string   `json:"name"`
	Tags    []string `json:"tags"`
	Addr    Address  `json:"addr"`
	private string   // 未导出字段：encoding/json 忽略
}

// TestBytesStructRoundtrip 覆盖结构体、嵌套结构体、结构体切片、映射与指针。
func TestBytesStructRoundtrip(t *testing.T) {
	u := User{
		ID:      7,
		Name:    "alice",
		Tags:    []string{"admin", "beta"},
		Addr:    Address{City: "Shanghai", Zip: "200000"},
		private: "ignored",
	}
	got, err := kvdb.P[User](kvdb.B(u))
	if err != nil {
		t.Fatalf("P[User]: %v", err)
	}
	if got.ID != u.ID || got.Name != u.Name || got.Addr != u.Addr || len(got.Tags) != 2 {
		t.Fatalf("结构体往返不一致: %+v", got)
	}
	if got.private != "" {
		t.Fatal("未导出字段不应被编码")
	}

	// 结构体切片
	list := []User{u, {ID: 8, Name: "bob"}}
	gotList, err := kvdb.P[[]User](kvdb.B(list))
	if err != nil || len(gotList) != 2 || gotList[1].Name != "bob" {
		t.Fatalf("[]User 往返 = %+v, %v", gotList, err)
	}

	// 映射
	m := map[string]int{"a": 1, "b": 2}
	gotMap, err := kvdb.P[map[string]int](kvdb.B(m))
	if err != nil || gotMap["b"] != 2 {
		t.Fatalf("map 往返 = %v, %v", gotMap, err)
	}

	// 指针
	gotPtr, err := kvdb.P[*User](kvdb.B(&u))
	if err != nil || gotPtr == nil || gotPtr.Name != "alice" {
		t.Fatalf("*User 往返 = %+v, %v", gotPtr, err)
	}

	// []byte 仍是恒等（不经过 JSON/base64）
	raw := []byte{0x00, 0xff, 'x'}
	if b := kvdb.B(raw); string(b) != string(raw) {
		t.Fatalf("[]byte 应恒等, got %v", b)
	}
	if gotRaw, err := kvdb.P[[]byte](raw); err != nil || string(gotRaw) != string(raw) {
		t.Fatalf("P[[]byte] = %v, %v", gotRaw, err)
	}

	// 标量不受结构体支持影响：int64 仍是十进制文本（与 Incr 互操作）
	if s := string(kvdb.B(int64(42))); s != "42" {
		t.Fatalf("int64 应为十进制文本, got %q", s)
	}
}

// TestBytesStructWithDB 验证结构体在真实读写链路（Set + Get + D）中可用。
func TestBytesStructWithDB(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "stub://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	want := User{ID: 9, Name: "carol", Tags: []string{"x"}, Addr: Address{City: "Beijing"}}
	if err := db.Set(ctx, "user:9", kvdb.B(want)); err != nil {
		t.Fatal(err)
	}
	got, err := kvdb.D[User](db.Get(ctx, "user:9"))
	if err != nil {
		t.Fatalf("D[User]: %v", err)
	}
	if got.ID != want.ID || got.Name != want.Name || got.Addr.City != "Beijing" {
		t.Fatalf("读回不一致: %+v", got)
	}
	// 缺失 -> ErrNotFound
	if _, err := kvdb.D[User](db.Get(ctx, "missing")); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("缺失应 ErrNotFound, got %v", err)
	}
	// DMust 变体
	must := kvdb.DMust[User](db.Get(ctx, "user:9"))
	if must.Name != "carol" {
		t.Fatalf("DMust = %+v", must)
	}
}

// TestBytesCustomCodec 验证编解码可在 init 中整体替换，且标量路径不受影响。
func TestBytesCustomCodec(t *testing.T) {
	origMarshal, origUnmarshal := kvdb.Marshal, kvdb.Unmarshal
	defer func() { kvdb.Marshal, kvdb.Unmarshal = origMarshal, origUnmarshal }()

	calls := 0
	// 自定义编解码：加前缀 + 走原 JSON（模拟 msgpack/gob 等替换）
	kvdb.Marshal = func(v any) ([]byte, error) {
		calls++
		b, err := origMarshal(v)
		if err != nil {
			return nil, err
		}
		return append([]byte("C:"), b...), nil
	}
	kvdb.Unmarshal = func(data []byte, out any) error {
		if len(data) < 2 || string(data[:2]) != "C:" {
			return errors.New("自定义编解码：缺少前缀")
		}
		return origUnmarshal(data[2:], out)
	}

	u := User{ID: 3, Name: "dave"}
	enc := kvdb.B(u)
	if string(enc[:2]) != "C:" {
		t.Fatalf("应使用自定义 Marshal, got %q", enc)
	}
	got, err := kvdb.P[User](enc)
	if err != nil || got.Name != "dave" {
		t.Fatalf("自定义解码失败: %+v, %v", got, err)
	}
	if calls == 0 {
		t.Fatal("自定义 Marshal 未被调用")
	}
	// 标量仍走文本编码，不经自定义编解码
	if s := string(kvdb.B(int64(5))); s != "5" {
		t.Fatalf("标量不应经过自定义 Marshal, got %q", s)
	}
	if n, err := kvdb.P[int64]([]byte("5")); err != nil || n != 5 {
		t.Fatalf("标量解码 = %d, %v", n, err)
	}
	// 解码失败时错误应向上传播
	if _, err := kvdb.P[User]([]byte("no-prefix")); err == nil {
		t.Fatal("缺少自定义前缀应返回错误")
	}
}

// TestBytesEncodeErrors 覆盖编码/解码失败路径。
func TestBytesEncodeErrors(t *testing.T) {
	// 不可 JSON 编码的类型：B 会 panic
	func() {
		defer func() {
			if recover() == nil {
				t.Error("B 编码不可序列化值应 panic")
			}
		}()
		kvdb.B(make(chan int))
	}()

	// 解码非法 JSON：P 返回错误
	if _, err := kvdb.P[User]([]byte("{not json")); err == nil {
		t.Fatal("非法 JSON 应返回错误")
	}
	// 类型不匹配：JSON 数字解码进 string 字段
	if _, err := kvdb.P[User]([]byte(`{"id":"not-a-number"}`)); err == nil {
		t.Fatal("字段类型不匹配应返回错误")
	}
}
