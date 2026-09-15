// 白盒测试：直接检查连接池的名额预留与计数不变式。
package ssdb

import (
	"sync"
	"testing"
)

// TestReserveNeverExceedsMax 验证 CAS 预留的两条不变式：
//  1. 并发预留的成功次数恰好等于上限（不会超发）；
//  2. 预留失败不改变 total（旧的 Load()+Add(1) 组合会在失败分支永久虚增
//     计数，让连接池再也补不满容量）。
func TestReserveNeverExceedsMax(t *testing.T) {
	const max = 4
	p := &Provider{max: max}

	const goroutines = 64
	var wg sync.WaitGroup
	var okCount int32
	var mu sync.Mutex
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if p.reserve() {
				mu.Lock()
				okCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if okCount != max {
		t.Fatalf("并发预留成功次数 = %d, want %d", okCount, max)
	}
	if got := p.total.Load(); got != max {
		t.Fatalf("预留后 total = %d, want %d", got, max)
	}

	// 已满时反复预留失败，total 必须保持不变（不得虚增）。
	for i := 0; i < 100; i++ {
		if p.reserve() {
			t.Fatalf("已达上限仍预留成功")
		}
	}
	if got := p.total.Load(); got != max {
		t.Fatalf("失败预留后 total = %d, want %d（计数被虚增）", got, max)
	}

	// 归还一个名额后应能再次预留。
	p.total.Add(-1)
	if !p.reserve() {
		t.Fatal("归还名额后应能预留")
	}
	if got := p.total.Load(); got != max {
		t.Fatalf("再次预留后 total = %d, want %d", got, max)
	}
}
