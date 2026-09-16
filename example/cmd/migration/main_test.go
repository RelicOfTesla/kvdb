package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/RelicOfTesla/kvdb"
)

// memDB 打开一个内存基座（mem:// 每次 Open 都是独立实例）。
func memDB(t *testing.T) kvdb.DB {
	t.Helper()
	db, err := kvdb.Open(context.Background(), "mem://")
	if err != nil {
		t.Fatalf("open mem://: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestMigrateKVAll: 全量搬运 + 值原样（含二进制）+ 计数正确。
func TestMigrateKVAll(t *testing.T) {
	src := memDB(t)
	ctx := context.Background()
	want := map[string][]byte{}
	for i := 0; i < 25; i++ {
		k := fmt.Sprintf("k%02d", i)
		v := []byte(fmt.Sprintf("v-%d", i))
		want[k] = v
		if err := src.Set(ctx, k, v); err != nil {
			t.Fatal(err)
		}
	}
	// 二进制值必须原样往返
	bin := []byte{0x00, 0xff, '\n', '\r', 0x01}
	if err := src.Set(ctx, "zz-bin", bin); err != nil {
		t.Fatal(err)
	}
	want["zz-bin"] = bin

	// mem:// 每次 Open 都是独立实例，因此同测试内建两个库即为"跨库"。
	dst := memDB(t)
	m := &migrator{src: src, dst: dst, opt: options{batchSize: 7}, out: os.Stdout}
	if err := m.kv(ctx); err != nil {
		t.Fatalf("kv: %v", err)
	}
	if m.nKV != int64(len(want)) {
		t.Fatalf("搬运数 = %d, want %d", m.nKV, len(want))
	}
	for k, v := range want {
		got, ok, err := dst.Get(ctx, k)
		if err != nil || !ok || string(got) != string(v) {
			t.Fatalf("目标 %s = %q,%v,%v; want %q", k, got, ok, err, v)
		}
	}
}

// TestMigrateKVPagination: 分页必须正确推进——这是最易出错的地方。
// 覆盖：批次恰好整除、余数为 1、以及 batchSize 大于总数（末页只剩重复的 start 项）。
func TestMigrateKVPagination(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ n, batch int }{
		{0, 5},  // 空源
		{1, 5},  // 单条，batch 大于总数
		{5, 5},  // 恰好整除
		{6, 5},  // 余 1（末页只有重复的 start → 必须能停）
		{10, 5}, // 整除两页
		{11, 3}, // 余 2
		{100, 7},
	} {
		t.Run(fmt.Sprintf("n=%d,batch=%d", tc.n, tc.batch), func(t *testing.T) {
			src := memDB(t)
			dst := memDB(t)
			for i := 0; i < tc.n; i++ {
				if err := src.Set(ctx, fmt.Sprintf("k%03d", i), []byte("v")); err != nil {
					t.Fatal(err)
				}
			}
			m := &migrator{src: src, dst: dst, opt: options{batchSize: tc.batch}, out: os.Stdout}
			// 死循环会让本测试挂住（go test 超时即失败），这正是在防 pagination 不推进。
			if err := m.kv(ctx); err != nil {
				t.Fatalf("kv: %v", err)
			}
			if m.nKV != int64(tc.n) {
				t.Fatalf("搬运数 = %d, want %d", m.nKV, tc.n)
			}
		})
	}
}

// TestMigrateKVPreservesTTL: 剩余 TTL 必须被保留（而不是丢 TTL 或重算满 TTL）。
func TestMigrateKVPreservesTTL(t *testing.T) {
	ctx := context.Background()
	src := memDB(t)
	dst := memDB(t)

	if err := src.Set(ctx, "plain", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := src.SetEx(ctx, "withttl", []byte("v"), 1000); err != nil {
		t.Fatal(err)
	}

	m := &migrator{src: src, dst: dst, opt: options{batchSize: 10}, out: os.Stdout}
	if err := m.kv(ctx); err != nil {
		t.Fatalf("kv: %v", err)
	}

	// 无 TTL 的 key 在目标上也不该有 TTL
	if _, has, _ := dst.TTL(ctx, "plain"); has {
		t.Fatal("无 TTL 的 key 不应在目标上产生 TTL")
	}
	// 有 TTL 的应保留（剩余量不增加：不超过源上的 1000）
	secs, has, err := dst.TTL(ctx, "withttl")
	if err != nil || !has {
		t.Fatalf("目标 withttl 应有 TTL: has=%v err=%v", has, err)
	}
	if secs <= 0 || secs > 1000 {
		t.Fatalf("保留的 TTL 应在 (0,1000] 内, got %d", secs)
	}
}

// TestMigrateKVSkipsTTLForExpired: 源上已过期的 key 不该被搬到目标。
func TestMigrateKVSkipsTTLForExpired(t *testing.T) {
	ctx := context.Background()
	src := memDB(t)
	dst := memDB(t)
	// 用过去时刻的绝对 TTL → 源上等同已删除
	if err := src.SetExAt(ctx, "dead", []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	if err := src.Set(ctx, "live", []byte("v")); err != nil {
		t.Fatal(err)
	}
	m := &migrator{src: src, dst: dst, opt: options{batchSize: 10}, out: os.Stdout}
	if err := m.kv(ctx); err != nil {
		t.Fatalf("kv: %v", err)
	}
	if _, ok, _ := dst.Get(ctx, "dead"); ok {
		t.Fatal("源上已过期的 key 不应被搬到目标")
	}
	if _, ok, _ := dst.Get(ctx, "live"); !ok {
		t.Fatal("正常 key 应被搬运")
	}
}

// TestMigratePrefix: 只搬指定前缀。
func TestMigratePrefix(t *testing.T) {
	ctx := context.Background()
	src := memDB(t)
	dst := memDB(t)
	for _, k := range []string{"user:1", "user:2", "post:1"} {
		if err := src.Set(ctx, k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	m := &migrator{src: src, dst: dst, opt: options{prefix: "user:", batchSize: 10}, out: os.Stdout}
	if err := m.kv(ctx); err != nil {
		t.Fatalf("kv: %v", err)
	}
	if m.nKV != 2 {
		t.Fatalf("前缀过滤后应搬 2 条, got %d", m.nKV)
	}
	if _, ok, _ := dst.Get(ctx, "post:1"); ok {
		t.Fatal("不匹配前缀的 key 不应被搬运")
	}
}

// TestMigrateZSet: 成员与分数（含负分）完整搬运。
func TestMigrateZSet(t *testing.T) {
	ctx := context.Background()
	src := memDB(t)
	dst := memDB(t)
	for _, m := range []struct {
		member string
		score  int64
	}{{"a", 10}, {"b", -5}, {"c", 0}} {
		if err := src.ZSet(ctx, "rank", m.member, m.score); err != nil {
			t.Fatal(err)
		}
	}
	m := &migrator{src: src, dst: dst, opt: options{zsets: []string{"rank"}}, out: os.Stdout}
	if err := m.zsets(ctx); err != nil {
		t.Fatalf("zsets: %v", err)
	}
	for _, want := range []struct {
		member string
		score  int64
	}{{"a", 10}, {"b", -5}, {"c", 0}} {
		s, ok, err := dst.ZGet(ctx, "rank", want.member)
		if err != nil || !ok || s != want.score {
			t.Fatalf("目标 %s = %d,%v,%v; want %d", want.member, s, ok, err, want.score)
		}
	}
}

// TestMigrateQueueRequiresOptIn: 队列默认**跳过**（因为读全队列必须破坏源），
// 加 -allow-queue-drain 后才搬运，且顺序与源一致。
func TestMigrateQueueRequiresOptIn(t *testing.T) {
	ctx := context.Background()

	// 默认：跳过，且源队列保持不动
	src := memDB(t)
	dst := memDB(t)
	for _, v := range []string{"a", "b", "c"} {
		if err := src.QPush(ctx, "jobs", []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	m := &migrator{src: src, dst: dst, opt: options{queues: []string{"jobs"}}, out: os.Stdout}
	if err := m.queues(ctx); err != nil {
		t.Fatalf("queues: %v", err)
	}
	if m.nQ != 0 {
		t.Fatalf("默认不应搬运队列, got %d", m.nQ)
	}
	if n, _ := src.QSize(ctx, "jobs"); n != 3 {
		t.Fatalf("默认模式下源队列不应被改动, got %d", n)
	}
	if len(m.skipped) == 0 {
		t.Fatal("应给出跳过原因")
	}

	// 显式允许：搬运且顺序一致（源队头 -> 目标入队顺序）
	src2 := memDB(t)
	dst2 := memDB(t)
	for _, v := range []string{"a", "b", "c"} {
		if err := src2.QPush(ctx, "jobs", []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	m2 := &migrator{src: src2, dst: dst2,
		opt: options{queues: []string{"jobs"}, allowDrain: true}, out: os.Stdout}
	if err := m2.queues(ctx); err != nil {
		t.Fatalf("queues(drain): %v", err)
	}
	if m2.nQ != 3 {
		t.Fatalf("应搬 3 条, got %d", m2.nQ)
	}
	for _, want := range []string{"a", "b", "c"} {
		v, ok, err := dst2.QPop(ctx, "jobs")
		if err != nil || !ok || string(v) != want {
			t.Fatalf("目标队列顺序 = %q,%v,%v; want %q", v, ok, err, want)
		}
	}
}

// TestMigrateDryRunWritesNothing: dry-run 只统计、不写目标。
func TestMigrateDryRunWritesNothing(t *testing.T) {
	ctx := context.Background()
	src := memDB(t)
	dst := memDB(t)
	for i := 0; i < 5; i++ {
		if err := src.Set(ctx, fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	m := &migrator{src: src, dst: dst, opt: options{dryRun: true, batchSize: 2}, out: os.Stdout}
	if err := m.kv(ctx); err != nil {
		t.Fatalf("kv: %v", err)
	}
	if m.nKV != 5 {
		t.Fatalf("dry-run 应统计 5 条, got %d", m.nKV)
	}
	// 目标应为空
	kvs, err := dst.Scan(ctx, "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 0 {
		t.Fatalf("dry-run 不应写入目标, got %d 条", len(kvs))
	}
}

// TestSplitNames: 逗号列表解析（去空白/去空项/去重/排序）。
func TestSplitNames(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{"a", []string{"a"}},
		{"a,b", []string{"a", "b"}},
		{" b , a ", []string{"a", "b"}}, // 去空白 + 排序
		{"a,,b,", []string{"a", "b"}},   // 去空项
		{"a,a,b", []string{"a", "b"}},   // 去重
	} {
		got := splitNames(tc.in)
		if len(got) != len(tc.want) {
			t.Fatalf("splitNames(%q) = %q, want %q", tc.in, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("splitNames(%q)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
}

// TestMigrateUnsupportedBatchFallsBack: 目标不支持 Batch 时应退回逐条，
// 而不是整类失败（迁移工具不该因目标缺一个可选能力就罢工）。
func TestMigrateUnsupportedBatchFallsBack(t *testing.T) {
	ctx := context.Background()
	src := memDB(t)
	if err := src.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	// 一个不支持 Batch 的目标：嵌入 KvProvider，不实现 Batcher。
	noBatch := &kvOnlyProvider{m: map[string][]byte{}}
	m := &migrator{src: src, dst: kvdb.Wrap(noBatch), opt: options{batchSize: 10}, out: os.Stdout}
	// Wrap 会探测 Batcher：KvProvider-only 的底层没有 Batch，故 dst.Batch 返回 ErrUnsupported。
	if err := m.kv(ctx); err != nil {
		t.Fatalf("kv 应回退逐条而不失败: %v", err)
	}
	if v, ok := noBatch.m["k"]; !ok || string(v) != "v" {
		t.Fatalf("回退写失败: %q,%v", v, ok)
	}
}

// kvOnlyProvider 是最小 KvProvider（无 Batch/Queue/ZSet），用于验证回退路径。
type kvOnlyProvider struct{ m map[string][]byte }

func (p *kvOnlyProvider) Set(_ context.Context, key string, value []byte) error {
	p.m[key] = append([]byte(nil), value...)
	return nil
}
func (p *kvOnlyProvider) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	if ttl <= 0 {
		return kvdb.ErrInvalidTTL
	}
	return p.Set(ctx, key, value)
}
func (p *kvOnlyProvider) SetExAt(ctx context.Context, key string, value []byte, at int64) error {
	if at <= 0 {
		delete(p.m, key)
		return nil
	}
	return p.Set(ctx, key, value)
}
func (p *kvOnlyProvider) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, ok := p.m[key]
	return v, ok, nil
}
func (p *kvOnlyProvider) Del(_ context.Context, key string) error { delete(p.m, key); return nil }
func (p *kvOnlyProvider) Exists(_ context.Context, key string) (bool, error) {
	_, ok := p.m[key]
	return ok, nil
}
func (p *kvOnlyProvider) Incr(context.Context, string, int64) (int64, error) {
	return 0, kvdb.ErrNotInteger
}
func (p *kvOnlyProvider) MGet(context.Context, ...string) (map[string][]byte, error) {
	return map[string][]byte{}, nil
}
func (p *kvOnlyProvider) Scan(context.Context, string, string, int) ([]kvdb.KeyValue, error) {
	return nil, nil
}
func (p *kvOnlyProvider) Expire(context.Context, string, int64) error { return nil }
func (p *kvOnlyProvider) ExpireAt(context.Context, string, int64) error {
	return nil
}
func (p *kvOnlyProvider) TTL(context.Context, string) (int64, bool, error) {
	return -1, false, nil
}
