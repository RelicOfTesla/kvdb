package sqlite_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/sqlite"
)

// TestConcurrentCloseAndOps 验证去掉 Close 的互斥锁后仍然安全：
// closed 是 atomic.Bool + Swap，幂等关闭不依赖锁；与并发读写竞争不得 panic、
// 不得数据竞争（配合 -race），只允许返回 ErrClosed 或成功。
func TestConcurrentCloseAndOps(t *testing.T) {
	ctx := context.Background()
	p, err := sqlite.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := "c:" + string(rune('a'+id))
			for i := 0; i < 200; i++ {
				if err := p.Set(ctx, key, []byte("v")); err != nil && !errors.Is(err, core.ErrClosed) {
					t.Errorf("Set: %v", err)
					return
				}
				if _, _, err := p.Get(ctx, key); err != nil && !errors.Is(err, core.ErrClosed) {
					t.Errorf("Get: %v", err)
					return
				}
			}
		}(g)
	}
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	wg.Wait()
	if _, _, err := p.Get(ctx, "c:a"); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("关闭后应稳定 ErrClosed, got %v", err)
	}
}
