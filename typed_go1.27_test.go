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
	db, err := kvdb.Open(ctx, "stub://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tdb := kvdb.Typed(db)

	// 结构体（默认 JSON）
	want := tUser{ID: 7, Name: "alice", Tags: []string{"a", "b"}}
	if err := db.Set(ctx, "u:7", kvdb.Enc(want)); err != nil {
		t.Fatal(err)
	}
	got, ok, err := tdb.Get[tUser](ctx, "u:7")
	if err != nil || !ok {
		t.Fatalf("Get[tUser]: ok=%v err=%v", ok, err)
	}
	if got.ID != want.ID || got.Name != want.Name || len(got.Tags) != 2 {
		t.Fatalf("结构体不一致: %+v", got)
	}

	// 标量：与 Incr 互操作
	if _, err := db.Incr(ctx, "visits", 5); err != nil {
		t.Fatal(err)
	}
	n, ok, err := tdb.Get[int64](ctx, "visits")
	if err != nil || !ok || n != 5 {
		t.Fatalf("Get[int64] = %d,%v,%v", n, ok, err)
	}
	if _, err := db.Incr(ctx, "visits", 2); err != nil {
		t.Fatal(err)
	}
	if n, ok, err := tdb.Get[int64](ctx, "visits"); err != nil || !ok || n != 7 {
		t.Fatalf("Incr 后 Get[int64] = %d,%v,%v", n, ok, err)
	}

	// 缺失：ok=false 且 err=nil（与 D 一致，不折成 ErrNotFound）
	if v, ok, err := tdb.Get[tUser](ctx, "missing"); err != nil || ok || v.ID != 0 {
		t.Fatalf("缺失应为零值,false,nil，got %+v,%v,%v", v, ok, err)
	}
	if v, ok, err := tdb.Get[string](ctx, "missing"); err != nil || ok || v != "" {
		t.Fatalf("缺失(string) = %q,%v,%v", v, ok, err)
	}
	if v, ok, err := tdb.Get[int64](ctx, "visits"); err != nil || !ok || v != 7 {
		t.Fatalf("Get = %d,%v,%v", v, ok, err)
	}

	// MGet 批量解码（结构体 + 标量混合场景）
	db.Set(ctx, "u:8", kvdb.Enc(tUser{ID: 8, Name: "bob"}))
	ms, err := tdb.MGet[tUser](ctx, "u:7", "u:8", "missing")
	if err != nil || len(ms) != 2 || ms["u:8"].Name != "bob" {
		t.Fatalf("MGet[tUser] = %+v, %v", ms, err)
	}
}

// TestTypedSet 验证写方向的泛型形态：Set/SetEx 接受 value T 并按 Enc[T] 编码。
func TestTypedSet(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "stub://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tdb := kvdb.Typed(db)

	// 结构体：写入即可读回，且与手动 Enc[T] 编码的字节完全一致
	u := tUser{ID: 7, Name: "alice", Tags: []string{"a"}}
	if err := tdb.Set(ctx, "u:7", u); err != nil {
		t.Fatalf("Set[tUser]: %v", err)
	}
	if v, ok, _ := db.Get(ctx, "u:7"); !ok || string(v) != string(kvdb.Enc(u)) {
		t.Fatalf("Set 编码应等于 Enc[tUser]")
	}
	if got, ok, err := tdb.Get[tUser](ctx, "u:7"); err != nil || !ok || got.Name != "alice" {
		t.Fatalf("回读 = %+v,%v,%v", got, ok, err)
	}

	// 标量：与 Incr 互操作（文本编码）
	if err := tdb.Set(ctx, "cnt", int64(41)); err != nil {
		t.Fatal(err)
	}
	if n, err := db.Incr(ctx, "cnt", 1); err != nil || n != 42 {
		t.Fatalf("Set(int64) 后 Incr = %d, %v", n, err)
	}
	if n, ok, err := tdb.Get[int64](ctx, "cnt"); err != nil || !ok || n != 42 {
		t.Fatalf("Get[int64] = %d,%v,%v", n, ok, err)
	}

	// SetEx 走基座的 SetEx（桩的 fakeKV.SetEx 不实现 TTL，只验证调用被正确透传；
	// TTL 语义由各基座模块的用例覆盖）
	if err := tdb.SetEx(ctx, "tmp", "v", 100); err != nil {
		t.Fatalf("SetEx[string]: %v", err)
	}
	if v, ok, _ := db.Get(ctx, "tmp"); !ok || string(v) != "v" {
		t.Fatalf("SetEx 后 Get = %q,%v", v, ok)
	}
	if err := tdb.SetEx(ctx, "tmp", "v2", 100); err != nil {
		t.Fatalf("SetEx 覆盖: %v", err)
	}
	if v, ok, _ := db.Get(ctx, "tmp"); !ok || string(v) != "v2" {
		t.Fatalf("SetEx 覆盖后 Get = %q,%v", v, ok)
	}
}

// TestTypedSetBytes 验证 T=[]byte 时与直接调用基座方法**完全等价**：
// B/encodeScalar 对 []byte 恒等透传，不引入任何包装或拷贝语义。
func TestTypedSetBytes(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "stub://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tdb := kvdb.Typed(db)

	raw := []byte{0x00, 0xff, 0x01, 0xfe, 'x'} // 含非 UTF-8 字节
	if err := tdb.Set(ctx, "bin", raw); err != nil {
		t.Fatal(err)
	}
	got, ok, err := db.Get(ctx, "bin")
	if err != nil || !ok {
		t.Fatalf("Get = %v,%v", ok, err)
	}
	if string(got) != string(raw) {
		t.Fatalf("T=[]byte 应原样透传: got %x want %x", got, raw)
	}

	// 队列同理
	if err := tdb.QPush(ctx, "q", raw); err != nil {
		t.Fatal(err)
	}
	qv, ok, err := db.QPop(ctx, "q")
	if err != nil || !ok || string(qv) != string(raw) {
		t.Fatalf("QPush([]byte) 应原样透传: %x,%v,%v", qv, ok, err)
	}
}

// TestTypedQueue 验证队列读写的泛型形态。
func TestTypedQueue(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "stub://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tdb := kvdb.Typed(db)

	// 用泛型 QPush 入队（写方向），再用泛型读回
	j1 := tUser{ID: 1, Name: "job-1"}
	j2 := tUser{ID: 2, Name: "job-2"}
	if err := tdb.QPush(ctx, "jobs", j1); err != nil {
		t.Fatalf("QPush[tUser]: %v", err)
	}
	if err := tdb.QPushFront(ctx, "jobs", j2); err != nil {
		t.Fatalf("QPushFront[tUser]: %v", err)
	}

	// 队头应为 QPushFront 的 j2
	if got, ok, err := tdb.QFront[tUser](ctx, "jobs"); err != nil || !ok || got.Name != "job-2" {
		t.Fatalf("QFront = %+v,%v,%v", got, ok, err)
	}
	if got, ok, err := tdb.QBack[tUser](ctx, "jobs"); err != nil || !ok || got.Name != "job-1" {
		t.Fatalf("QBack = %+v,%v,%v", got, ok, err)
	}
	if got, ok, err := tdb.QPop[tUser](ctx, "jobs"); err != nil || !ok || got.ID != 2 {
		t.Fatalf("QPop = %+v,%v,%v", got, ok, err)
	}
	if got, ok, err := tdb.QPopBack[tUser](ctx, "jobs"); err != nil || !ok || got.ID != 1 {
		t.Fatalf("QPopBack = %+v,%v,%v", got, ok, err)
	}
	// 空队列 -> ok=false 且 err=nil（与 D 一致，不再折算为 ErrNotFound）
	if _, ok, err := tdb.QPop[tUser](ctx, "jobs"); err != nil || ok {
		t.Fatalf("空队列应 ok=false 且无错, got ok=%v err=%v", ok, err)
	}
}

// TestTypedQRange 验证区间读取的泛型形态：逐元素解码、保序、只读。
func TestTypedQRange(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "stub://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tdb := kvdb.Typed(db)

	for i := 1; i <= 5; i++ {
		if err := tdb.QPush(ctx, "qr", tUser{ID: int64(i), Name: "u"}); err != nil {
			t.Fatal(err)
		}
	}
	// 全区间
	all, err := tdb.QRange[tUser](ctx, "qr", 0, -1)
	if err != nil || len(all) != 5 {
		t.Fatalf("QRange(0,-1) = %d 项, err=%v", len(all), err)
	}
	for i, u := range all {
		if u.ID != int64(i+1) {
			t.Fatalf("QRange[%d].ID = %d, want %d（须保序）", i, u.ID, i+1)
		}
	}
	// 负索引 / 越界裁剪
	tail, err := tdb.QRange[tUser](ctx, "qr", -2, -1)
	if err != nil || len(tail) != 2 || tail[0].ID != 4 || tail[1].ID != 5 {
		t.Fatalf("QRange(-2,-1) = %+v, err=%v", tail, err)
	}
	if got, err := tdb.QRange[tUser](ctx, "qr", 0, 100); err != nil || len(got) != 5 {
		t.Fatalf("QRange(0,100) 应裁剪为 5 项, got %d err=%v", len(got), err)
	}
	if got, err := tdb.QRange[tUser](ctx, "qr", 3, 1); err != nil || len(got) != 0 {
		t.Fatalf("空区间应为空, got %d err=%v", len(got), err)
	}
	// 空区间：空且无错。
	// 注意：这里不断言"另一个队列名为空"——根部测试桩的 fakeQueue 忽略队列名
	// （用单一共享列表），那属于测试替身的简化，不是产品行为。
	if got, err := tdb.QRange[tUser](ctx, "qr", 10, 20); err != nil || len(got) != 0 {
		t.Fatalf("越界区间应空且无错, got %d err=%v", len(got), err)
	}
	// 只读：长度不变
	if n, err := db.QSize(ctx, "qr"); err != nil || n != 5 {
		t.Fatalf("QRange 后 QSize = %d, err=%v", n, err)
	}
	// 标量元素同样可用（读末位：测试桩的队列忽略名字，是单一共享列表）
	if err := tdb.QPush(ctx, "qr", int64(42)); err != nil {
		t.Fatal(err)
	}
	if got, err := tdb.QRange[int64](ctx, "qr", -1, -1); err != nil || len(got) != 1 || got[0] != 42 {
		t.Fatalf("QRange[int64] = %v, err=%v", got, err)
	}
	// 解码失败必须报错（区间可能非空，无法用 ok 表达）。
	// 往末尾追加一个非法 JSON 元素，再读最后一个位置即可命中它。
	if err := db.QPush(ctx, "qr", []byte("not-json")); err != nil {
		t.Fatal(err)
	}
	if _, err := tdb.QRange[tUser](ctx, "qr", -1, -1); err == nil {
		t.Fatal("区间内存在不可解码元素时应报错")
	}
}

// TestTypedSetExAt 验证绝对到期时刻的泛型形态。
func TestTypedSetExAt(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "stub://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tdb := kvdb.Typed(db)

	// 未来时刻：写入且可读
	u := tUser{ID: 9, Name: "future"}
	at := core.NowUnix() + 100
	if err := tdb.SetExAt(ctx, "at", u, at); err != nil {
		t.Fatalf("SetExAt(未来): %v", err)
	}
	if got, ok, err := tdb.Get[tUser](ctx, "at"); err != nil || !ok || got.ID != 9 {
		t.Fatalf("SetExAt 后 Get = %+v,%v,%v", got, ok, err)
	}
	// 桩的 TTL 恒返回 ok=false（不建模 TTL 表），故此处不校验 TTL 记录；
	// 绝对时刻的语义由 kvdbtest 的 TTLBoundaries 在各真实基座上覆盖。
	// 过去时刻：删除该 key
	if err := tdb.SetExAt(ctx, "at", u, core.NowUnix()-1); err != nil {
		t.Fatalf("SetExAt(过去): %v", err)
	}
	if _, ok, _ := tdb.Get[tUser](ctx, "at"); ok {
		t.Fatal("SetExAt(过去) 应删除该 key")
	}
}

// TestTypedOKVariants 验证 *OK 系列**保留 ok 原值**：空/缺失时 ok=false 且 err=nil，
// 与不带 OK 后缀的形态（把"空"折算为 ErrNotFound）区分开。
func TestTypedReadSignature(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "stub://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tdb := kvdb.Typed(db)

	// 读方法的签名与 D 一致：缺失/空队列给 ok=false + err=nil，不折成 ErrNotFound
	if v, ok, err := tdb.Get[string](ctx, "missing"); err != nil || ok || v != "" {
		t.Fatalf("Get 缺失应为 \"\",false,nil，got %q,%v,%v", v, ok, err)
	}

	// 四个队列读方法同签名
	for _, tc := range []struct {
		name string
		call func() (string, bool, error)
	}{
		{"QPop", func() (string, bool, error) { return tdb.QPop[string](ctx, "empty") }},
		{"QPopBack", func() (string, bool, error) { return tdb.QPopBack[string](ctx, "empty") }},
		{"QFront", func() (string, bool, error) { return tdb.QFront[string](ctx, "empty") }},
		{"QBack", func() (string, bool, error) { return tdb.QBack[string](ctx, "empty") }},
	} {
		v, ok, err := tc.call()
		if err != nil || ok || v != "" {
			t.Fatalf("%s 空队列应为 \"\",false,nil，got %q,%v,%v", tc.name, v, ok, err)
		}
	}

	// 有值时正常返回 ok=true
	if err := tdb.QPush(ctx, "jobs", "job-1"); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := tdb.QFront[string](ctx, "jobs"); err != nil || !ok || v != "job-1" {
		t.Fatalf("QFront 有值 = %q,%v,%v", v, ok, err)
	}
	if v, ok, err := tdb.QPop[string](ctx, "jobs"); err != nil || !ok || v != "job-1" {
		t.Fatalf("QPop 有值 = %q,%v,%v", v, ok, err)
	}
	// 解析错误仍须上报为 err（不是 ok=false）
	if err := db.Set(ctx, "bad", []byte("not-an-int")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := tdb.Get[int64](ctx, "bad"); err == nil || ok {
		t.Fatalf("非整数应报 err 且 ok=false，got ok=%v err=%v", ok, err)
	}
}

// TestTypedBatch 验证类型化批收集：收集时即编码，且**同一批可混装多种类型**。
func TestTypedBatch(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "stub://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tdb := kvdb.Typed(db)

	err = tdb.BatchT(ctx, func(b kvdb.TypedBatch) error {
		b.Set("u:1", tUser{ID: 1, Name: "a"}) // T = tUser
		b.Set("u:2", tUser{ID: 2, Name: "b"})
		b.Set("cnt", 42) // T = int —— 同批混装
		b.SetEx("u:3", tUser{ID: 3, Name: "c"}, 100)
		b.QPush("jobs", tUser{ID: 4, Name: "d"})
		// 不涉及 value 的方法经内嵌的 *Batch 直接可用（真正生效，不是摆设）
		b.Del("nonexistent")
		b.Expire("u:1", 50)
		if b.Len() != 7 {
			t.Fatalf("批内操作数 = %d, want 7", b.Len())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("BatchT: %v", err)
	}

	for _, tc := range []struct {
		key  string
		want string
	}{{"u:1", "a"}, {"u:2", "b"}, {"u:3", "c"}} {
		got, ok, err := tdb.Get[tUser](ctx, tc.key)
		_ = ok
		if err != nil || got.Name != tc.want {
			t.Fatalf("批内 %s = %+v, %v", tc.key, got, err)
		}
	}
	if got, ok, err := tdb.QPop[tUser](ctx, "jobs"); err != nil || !ok || got.ID != 4 {
		t.Fatalf("批内 QPush 后 QPop = %+v,%v,%v", got, ok, err)
	}
	if n, ok, err := tdb.Get[int](ctx, "cnt"); err != nil || !ok || n != 42 {
		t.Fatalf("批内混装的 int = %d,%v,%v", n, ok, err)
	}
}

// TestTypedBatchAtomic 验证批内错误语义仍由 Batch 负责（收集期报错导致整批不提交）。
func TestTypedBatchAtomic(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "stub://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tdb := kvdb.Typed(db)

	sentinel := errors.New("abort")
	if err := tdb.BatchT(ctx, func(b kvdb.TypedBatch) error {
		b.Set("bk", "v")
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("回调错误应原样返回, got %v", err)
	}
	if ok, _ := db.Exists(ctx, "bk"); ok {
		t.Fatal("回调出错时整批不应提交")
	}
}

// TestTypedAcceptsStoreProvider 验证 Typed 只要求 StoreProvider：
// 一个**没有** Batch/Close 的三能力存储也能被类型化（StoreProvider 不含这两者）。
func TestTypedAcceptsStoreProvider(t *testing.T) {
	ctx := context.Background()
	store := newStoreOnlyStub()
	tdb := kvdb.Typed(store)

	if err := tdb.Set(ctx, "k", 42); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if n, ok, err := tdb.Get[int](ctx, "k"); err != nil || !ok || n != 42 {
		t.Fatalf("Get = %d,%v,%v", n, ok, err)
	}
	if err := tdb.QPush(ctx, "q", "job"); err != nil {
		t.Fatalf("QPush: %v", err)
	}
	if v, ok, err := tdb.QPop[string](ctx, "q"); err != nil || !ok || v != "job" {
		t.Fatalf("QPop = %q,%v,%v", v, ok, err)
	}
	if err := tdb.ZSet(ctx, "z", "m", 1); err != nil {
		t.Fatalf("ZSet: %v", err)
	}

	// 该存储不实现 Batcher：BatchT 必须报 ErrUnsupported（与 DB.Batch 契约一致），
	// 而不是 panic。
	err := tdb.BatchT(ctx, func(b kvdb.TypedBatch) error { return nil })
	if !errors.Is(err, core.ErrUnsupported) {
		t.Fatalf("无 Batch 能力时应 ErrUnsupported, got %v", err)
	}
}

// storeOnlyStub 满足 StoreProvider（KV + Queue + ZSet），刻意**不实现**
// Batch/Close，用于钉住"泛型壳不额外要求能力"。
type storeOnlyStub struct {
	kv    map[string][]byte
	queue map[string][][]byte
	zset  map[string]map[string]int64
}

func newStoreOnlyStub() *storeOnlyStub {
	return &storeOnlyStub{
		kv:    map[string][]byte{},
		queue: map[string][][]byte{},
		zset:  map[string]map[string]int64{},
	}
}

func (s *storeOnlyStub) Set(_ context.Context, key string, value []byte) error {
	s.kv[key] = value
	return nil
}
func (s *storeOnlyStub) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	return s.Set(ctx, key, value)
}
func (s *storeOnlyStub) SetExAt(ctx context.Context, key string, value []byte, at int64) error {
	if at <= core.NowUnix() {
		delete(s.kv, key)
		return nil
	}
	return s.Set(ctx, key, value)
}
func (s *storeOnlyStub) ExpireAt(_ context.Context, key string, at int64) error {
	if at <= core.NowUnix() {
		delete(s.kv, key)
	}
	return nil
}
func (s *storeOnlyStub) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, ok := s.kv[key]
	return v, ok, nil
}
func (s *storeOnlyStub) Del(_ context.Context, key string) error { delete(s.kv, key); return nil }
func (s *storeOnlyStub) Exists(_ context.Context, key string) (bool, error) {
	_, ok := s.kv[key]
	return ok, nil
}
func (s *storeOnlyStub) Incr(context.Context, string, int64) (int64, error) {
	return 0, core.ErrNotInteger
}
func (s *storeOnlyStub) MGet(context.Context, ...string) (map[string][]byte, error) {
	return map[string][]byte{}, nil
}
func (s *storeOnlyStub) Scan(context.Context, string, string, int) ([]kvdb.KeyValue, error) {
	return nil, nil
}
func (s *storeOnlyStub) Expire(context.Context, string, int64) error { return nil }
func (s *storeOnlyStub) TTL(context.Context, string) (int64, bool, error) {
	return -1, false, nil
}

func (s *storeOnlyStub) QPush(_ context.Context, name string, value []byte) error {
	s.queue[name] = append(s.queue[name], value)
	return nil
}
func (s *storeOnlyStub) QPushFront(_ context.Context, name string, value []byte) error {
	s.queue[name] = append([][]byte{value}, s.queue[name]...)
	return nil
}
func (s *storeOnlyStub) QPop(_ context.Context, name string) ([]byte, bool, error) {
	q := s.queue[name]
	if len(q) == 0 {
		return nil, false, nil
	}
	s.queue[name] = q[1:]
	return q[0], true, nil
}
func (s *storeOnlyStub) QPopBack(_ context.Context, name string) ([]byte, bool, error) {
	q := s.queue[name]
	if len(q) == 0 {
		return nil, false, nil
	}
	s.queue[name] = q[:len(q)-1]
	return q[len(q)-1], true, nil
}
func (s *storeOnlyStub) QSize(_ context.Context, name string) (int64, error) {
	return int64(len(s.queue[name])), nil
}
func (s *storeOnlyStub) QFront(_ context.Context, name string) ([]byte, bool, error) {
	q := s.queue[name]
	if len(q) == 0 {
		return nil, false, nil
	}
	return q[0], true, nil
}
func (s *storeOnlyStub) QBack(_ context.Context, name string) ([]byte, bool, error) {
	q := s.queue[name]
	if len(q) == 0 {
		return nil, false, nil
	}
	return q[len(q)-1], true, nil
}

func (s *storeOnlyStub) QRange(_ context.Context, name string, start, stop int64) ([][]byte, error) {
	q := s.queue[name]
	n := int64(len(q))
	if start < 0 {
		start += n
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += n
	}
	if n == 0 || start >= n || stop < start {
		return nil, nil
	}
	if stop >= n {
		stop = n - 1
	}
	out := make([][]byte, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, q[i])
	}
	return out, nil
}

func (s *storeOnlyStub) ZSet(_ context.Context, name, key string, score int64) error {
	if s.zset[name] == nil {
		s.zset[name] = map[string]int64{}
	}
	s.zset[name][key] = score
	return nil
}
func (s *storeOnlyStub) ZGet(_ context.Context, name, key string) (int64, bool, error) {
	v, ok := s.zset[name][key]
	return v, ok, nil
}
func (s *storeOnlyStub) ZDel(_ context.Context, name, key string) error {
	delete(s.zset[name], key)
	return nil
}
func (s *storeOnlyStub) ZSize(_ context.Context, name string) (int64, error) {
	return int64(len(s.zset[name])), nil
}
func (s *storeOnlyStub) ZRank(context.Context, string, string) (int64, bool, error) {
	return 0, false, nil
}
func (s *storeOnlyStub) ZRange(context.Context, string, int64, int64) ([]kvdb.ZItem, error) {
	return nil, nil
}
func (s *storeOnlyStub) ZRangeByScore(context.Context, string, int64, int64, int, bool) ([]kvdb.ZItem, error) {
	return nil, nil
}
func (s *storeOnlyStub) ZIncr(_ context.Context, name, key string, delta int64) (int64, error) {
	if s.zset[name] == nil {
		s.zset[name] = map[string]int64{}
	}
	s.zset[name][key] += delta
	return s.zset[name][key], nil
}

// 编译期确认：storeOnlyStub 满足 StoreProvider 但**不**满足 FullProvider。
var (
	_ kvdb.StoreProvider = (*storeOnlyStub)(nil)
	_                    = func() bool {
		var p any = (*storeOnlyStub)(nil)
		_, isFull := p.(kvdb.FullProvider)
		if isFull {
			panic("storeOnlyStub 不应满足 FullProvider（它没有 Batch/Close）")
		}
		return true
	}
)
