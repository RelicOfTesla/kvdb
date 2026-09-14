package kvdb_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/RelicOfTesla/kvdb"
)

// fakeKV 是只实现 KV 能力的最小 mock：证明业务函数可以只依赖窄接口
// （kvdb.KvProvider 等 core 能力接口），测试里无需实现 Queue/ZSet/Batch——这些
// 接口 core 已经提供，根包不再重复定义。
type fakeKV struct {
	data   map[string][]byte
	failOn string // 命中该 key 的 Set 返回错误
}

func newFakeKV() *fakeKV { return &fakeKV{data: map[string][]byte{}} }

func (f *fakeKV) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, ok := f.data[key]
	return v, ok, nil
}

func (f *fakeKV) Exists(_ context.Context, key string) (bool, error) {
	_, ok := f.data[key]
	return ok, nil
}

func (f *fakeKV) MGet(_ context.Context, keys ...string) (map[string][]byte, error) {
	out := map[string][]byte{}
	for _, k := range keys {
		if v, ok := f.data[k]; ok {
			out[k] = v
		}
	}
	return out, nil
}

func (f *fakeKV) Scan(_ context.Context, start, end string, _ int) ([]kvdb.KeyValue, error) {
	var out []kvdb.KeyValue
	for k, v := range f.data {
		if start != "" && k < start {
			continue
		}
		if end != "" && k > end {
			continue
		}
		out = append(out, kvdb.KeyValue{Key: k, Value: v})
	}
	return out, nil
}

func (f *fakeKV) TTL(_ context.Context, _ string) (int64, bool, error) { return -1, false, nil }

func (f *fakeKV) Set(_ context.Context, key string, value []byte) error {
	if f.failOn != "" && key == f.failOn {
		return errors.New("mock: set failed")
	}
	f.data[key] = value
	return nil
}

func (f *fakeKV) SetEx(ctx context.Context, key string, value []byte, _ int64) error {
	return f.Set(ctx, key, value)
}

func (f *fakeKV) Del(_ context.Context, key string) error {
	delete(f.data, key)
	return nil
}

func (f *fakeKV) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	cur, ok, err := f.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	var n int64
	if ok {
		if n, err = kvdb.P[int64](cur); err != nil {
			return 0, err
		}
	}
	n += delta
	if err := f.Set(ctx, key, kvdb.B(n)); err != nil {
		return 0, err
	}
	return n, nil
}

func (f *fakeKV) Expire(context.Context, string, int64) error { return nil }

// 业务函数只依赖 kvdb.KV——真实基座与 mock 都能传入。
func upsertUser(ctx context.Context, store kvdb.KvProvider, name string, visits int64) (int64, error) {
	if err := store.Set(ctx, "user:"+name, []byte(name)); err != nil {
		return 0, err
	}
	return store.Incr(ctx, "visits:"+name, visits)
}

// TestMockKV 验证 kvdb.KvProvider 接口可被最小 mock 替换（无需实现 Queue/ZSet/Batch）。
func TestMockKV(t *testing.T) {
	ctx := context.Background()
	mock := newFakeKV()

	n, err := upsertUser(ctx, mock, "alice", 3)
	if err != nil {
		t.Fatalf("upsertUser(mock): %v", err)
	}
	if n != 3 {
		t.Fatalf("mock Incr = %d, want 3", n)
	}
	if v, ok, _ := mock.Get(ctx, "user:alice"); !ok || string(v) != "alice" {
		t.Fatalf("mock Set 未生效: %q,%v", v, ok)
	}
	// 注入错误路径：mock 可精确控制失败
	mock.failOn = "user:bob"
	if _, err := upsertUser(ctx, mock, "bob", 1); err == nil {
		t.Fatal("mock 注入的错误应向上传播")
	}

	// 同一业务函数用真实基座（mem://）验证接口一致性
	db, err := kvdb.Open(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	real, err := upsertUser(ctx, db, "alice", 3) // DB 满足 KvProvider（组合接口）
	if err != nil {
		t.Fatalf("upsertUser(real): %v", err)
	}
	if real != 3 {
		t.Fatalf("real Incr = %d, want 3", real)
	}
}

// TestMockQueueOnly 验证只依赖队列子接口的 mock（不实现 KV）。
type fakeQueue struct{ items []string }

func (q *fakeQueue) QPush(_ context.Context, _ string, value []byte) error {
	q.items = append(q.items, string(value))
	return nil
}
func (q *fakeQueue) QPushFront(_ context.Context, _ string, value []byte) error {
	q.items = append([]string{string(value)}, q.items...)
	return nil
}
func (q *fakeQueue) QPop(_ context.Context, _ string) ([]byte, bool, error) {
	if len(q.items) == 0 {
		return nil, false, nil
	}
	v := q.items[0]
	q.items = q.items[1:]
	return []byte(v), true, nil
}
func (q *fakeQueue) QPopBack(_ context.Context, _ string) ([]byte, bool, error) {
	if len(q.items) == 0 {
		return nil, false, nil
	}
	v := q.items[len(q.items)-1]
	q.items = q.items[:len(q.items)-1]
	return []byte(v), true, nil
}
func (q *fakeQueue) QSize(_ context.Context, _ string) (int64, error) {
	return int64(len(q.items)), nil
}
func (q *fakeQueue) QFront(_ context.Context, _ string) ([]byte, bool, error) {
	if len(q.items) == 0 {
		return nil, false, nil
	}
	return []byte(q.items[0]), true, nil
}
func (q *fakeQueue) QBack(_ context.Context, _ string) ([]byte, bool, error) {
	if len(q.items) == 0 {
		return nil, false, nil
	}
	return []byte(q.items[len(q.items)-1]), true, nil
}

// drainQueue 只依赖 kvdb.QueueProvider。
func drainQueue(ctx context.Context, q kvdb.QueueProvider) ([]string, error) {
	var out []string
	for {
		v, ok, err := q.QPop(ctx, "jobs")
		if err != nil {
			return nil, err
		}
		if !ok {
			return out, nil
		}
		out = append(out, string(v))
	}
}

func TestMockQueueOnly(t *testing.T) {
	ctx := context.Background()
	mock := &fakeQueue{}
	mock.QPush(ctx, "jobs", []byte("a"))
	mock.QPush(ctx, "jobs", []byte("b"))
	mock.QPushFront(ctx, "jobs", []byte("z"))

	got, err := drainQueue(ctx, mock)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "z,a,b" {
		t.Fatalf("drainQueue(mock) = %v, want [z a b]", got)
	}
}

// otherDB 通过嵌入 DB 接口满足 kvdb.DB，用于验证非适配器实现的分支。
type otherDB struct{ kvdb.DB }

// TestUnwrap 验证 Unwrap 可取回底层基座（非适配器实现返回 nil）。
func TestUnwrap(t *testing.T) {
	ctx := context.Background()
	db, err := kvdb.Open(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if kvdb.Unwrap(db) == nil {
		t.Fatal("适配器应能取回底层基座")
	}
	// 以嵌入 DB 接口的替身模拟"非适配器实现"，Unwrap 应返回 nil。
	if kvdb.Unwrap(otherDB{}) != nil {
		t.Fatal("非适配器实现应返回 nil")
	}
}
