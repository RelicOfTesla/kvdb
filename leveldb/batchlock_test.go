package leveldb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb/core"
)

// 本文件回归缺陷 1：ApplyBatch 的批写锁在"两个名字哈希到同一分片"时对同一把
// sync.Mutex 二次 Lock 而**自死锁**（sync.Mutex 不可重入）。
//
// 之所以放在 package leveldb（白盒）而不是 leveldb_test：需要直接调用 keyShard /
// keyMutex / TryLock 来构造并验证"同分片"这一触发条件。

// findCollidingQueueZSet 找出一个队列名与一个 zset 名，使它们的**锁键**
// （"q:"+name 与 "z:"+name）落到同一分片，即同一批里会对同一把锁加锁两次。
func findCollidingQueueZSet(t *testing.T, p *Provider) (qname, zname string) {
	t.Helper()
	byShard := make(map[uint32]string, keyShards)
	for i := 0; i < 4096; i++ {
		n := fmt.Sprintf("cq%d", i)
		byShard[keyShard("q:"+n)] = n // 4096 个候选足以填满全部 64 个分片
	}
	if len(byShard) != keyShards {
		t.Fatalf("候选名仅覆盖 %d/%d 个分片", len(byShard), keyShards)
	}
	for i := 0; i < 4096; i++ {
		n := fmt.Sprintf("cz%d", i)
		if qn, ok := byShard[keyShard("z:"+n)]; ok {
			return qn, n
		}
	}
	t.Fatal("未能找到同分片的 q/z 名字")
	return "", ""
}

// findCollidingQueues 找出两个不同队列名，其锁键落到同一分片（同前缀碰撞）。
func findCollidingQueues(t *testing.T, p *Provider) (string, string) {
	t.Helper()
	byShard := make(map[uint32]string, keyShards)
	for i := 0; i < 4096; i++ {
		n := fmt.Sprintf("qq%d", i)
		s := keyShard("q:" + n)
		if prev, ok := byShard[s]; ok && prev != n {
			return prev, n
		}
		byShard[s] = n
	}
	t.Fatal("未能找到同分片的两个队列名")
	return "", ""
}

// runBatchWithTimeout 在独立 goroutine 里跑一次 ApplyBatch，用超时兜底把
// "自死锁"表现为一条明确的测试失败，而不是整个测试进程静默卡住
// （外层 `go test -timeout` 仍可兜底，但那条路径不会给出定位信息）。
//
// 说明：若真的超时，本用例只泄漏一个永久阻塞的 goroutine——它持有的是本用例
// **私有** provider 的分片锁（keyMu 是 per-Provider 数组），不影响同包其它用例。
func runBatchWithTimeout(t *testing.T, p *Provider, ops []core.BatchOp) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- p.ApplyBatch(context.Background(), ops) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ApplyBatch: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("ApplyBatch 超时（自死锁）：ops=%+v；请检查 lockKeys 是否按 mutex 去重并按分片全序加锁", ops)
	}
}

// TestBatchLockNoSelfDeadlock 构造"两个名字哈希到同一分片"，在同一批里操作它们，
// 断言不死锁且语义正确（修复前 lockKeys 会在第二次 Lock 上永久阻塞）。
func TestBatchLockNoSelfDeadlock(t *testing.T) {
	ctx := context.Background()
	p, err := Open(ctx, t.TempDir()+"/db", Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// 跨前缀碰撞：队列名与 zset 名的锁键同分片（如 "q:q4" 与 "z:z0"）。
	qn, zn := findCollidingQueueZSet(t, p)
	t.Logf("跨前缀同分片: q:%s 与 z:%s -> 分片 %d", qn, zn, keyShard("q:"+qn))
	runBatchWithTimeout(t, p, []core.BatchOp{
		{Kind: core.BatchQPush, Key: qn, Value: []byte("v")},
		{Kind: core.BatchZSet, Key: zn, Member: "m", Score: 7},
	})
	if n, err := p.QSize(ctx, qn); err != nil || n != 1 {
		t.Fatalf("QSize(%q) = %d, %v", qn, n, err)
	}
	if v, ok, err := p.QFront(ctx, qn); err != nil || !ok || string(v) != "v" {
		t.Fatalf("QFront(%q) = %q,%v,%v", qn, v, ok, err)
	}
	if s, ok, err := p.ZGet(ctx, zn, "m"); err != nil || !ok || s != 7 {
		t.Fatalf("ZGet(%q,m) = %d,%v,%v", zn, s, ok, err)
	}

	// 同前缀碰撞：两个不同队列名落到同一分片。
	q1, q2 := findCollidingQueues(t, p)
	t.Logf("同前缀同分片: q:%s 与 q:%s -> 分片 %d", q1, q2, keyShard("q:"+q1))
	runBatchWithTimeout(t, p, []core.BatchOp{
		{Kind: core.BatchQPush, Key: q1, Value: []byte("1")},
		{Kind: core.BatchQPushFront, Key: q2, Value: []byte("2")},
	})
	for _, n := range []string{q1, q2} {
		if got, err := p.QSize(ctx, n); err != nil || got != 1 {
			t.Fatalf("QSize(%q) = %d, %v", n, got, err)
		}
	}

	// 批结束后锁必须全部释放（去重后不会漏解锁）。
	for _, k := range []string{"q:" + qn, "z:" + zn, "q:" + q1, "q:" + q2} {
		m := p.keyMutex(k)
		if !m.TryLock() {
			t.Fatalf("批结束后锁未释放: %q", k)
		}
		m.Unlock()
	}
}
