package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

// TestConcurrentCloseAndOpsBroad 在更宽的操作面上重复"在途读写 + 并发 Close"：
// 若干 goroutine 轮转 Set/Get/Exists/Del/SetEx/TTL/QPush/QSize/QFront/QRange/
// ZSet/ZGet/ZSize/ZRank/ZRange，另一些 goroutine 调 Close。
//
// 断言：任何返回的错误都只能是 core.ErrClosed（errors.Is），错误文本绝不含
// 驱动原始的 "database is closed"；任何 goroutine 都不得 panic。
//
// 与 TestConcurrentCloseAndOps 的分工：后者只压 Set/Get（修复当时仅覆盖的两个
// 方法），本条把整个读写面都卷进来。并发时序本身不可完全复现，因此确定性回归在
// sqlstore/close_race_test.go——那里用最小驱动精确卡住 check()/Close() 窗口，对
// 每个方法单独断言；本条是"广撒网"补充，确保真实驱动下的并发路径也不漏。
func TestConcurrentCloseAndOpsBroad(t *testing.T) {
	ctx := context.Background()
	// 用文件库（WAL + 多连接）而非 :memory:（单连接）：并发读会真正占用连接，
	// 让 Close 与在途语句的交错更接近生产。
	p, err := sqlite.Open(ctx, t.TempDir()+"/race.db")
	if err != nil {
		t.Fatal(err)
	}

	// 每个 goroutine 单轮内做多种读写；nil 之外的错误只允许 ErrClosed。
	step := func(ctx context.Context, p *sqlite.Provider, id, i int) error {
		key := fmt.Sprintf("crb:%d", id)
		if err := p.Set(ctx, key, []byte("v")); err != nil {
			return err
		}
		if _, _, err := p.Get(ctx, key); err != nil {
			return err
		}
		if _, err := p.Exists(ctx, key); err != nil {
			return err
		}
		if err := p.SetEx(ctx, key, []byte("v"), 100); err != nil {
			return err
		}
		if _, _, err := p.TTL(ctx, key); err != nil {
			return err
		}
		if _, err := p.MGet(ctx, key, fmt.Sprintf("crb:%d", (id+1)%8)); err != nil {
			return err
		}
		if _, err := p.Scan(ctx, "crb:", "crb:~", 10); err != nil {
			return err
		}
		if err := p.QPush(ctx, "crb:q", []byte("v")); err != nil {
			return err
		}
		if _, err := p.QSize(ctx, "crb:q"); err != nil {
			return err
		}
		if _, _, err := p.QFront(ctx, "crb:q"); err != nil {
			return err
		}
		if _, err := p.QRange(ctx, "crb:q", 0, -1); err != nil {
			return err
		}
		if err := p.ZSet(ctx, "crb:z", "m", int64(i)); err != nil {
			return err
		}
		if _, _, err := p.ZGet(ctx, "crb:z", "m"); err != nil {
			return err
		}
		if _, err := p.ZSize(ctx, "crb:z"); err != nil {
			return err
		}
		if _, _, err := p.ZRank(ctx, "crb:z", "m"); err != nil {
			return err
		}
		if _, err := p.ZRange(ctx, "crb:z", 0, -1); err != nil {
			return err
		}
		if i%7 == 0 {
			if err := p.Del(ctx, key); err != nil {
				return err
			}
		}
		return nil
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// 协程内 panic 会让整个测试进程崩溃；转为 t.Errorf 才能给出定位。
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("并发操作 panic: %v", r)
				}
			}()
			for i := 0; i < 300; i++ {
				err := step(ctx, p, id, i)
				if err == nil || errors.Is(err, core.ErrClosed) {
					if err != nil && strings.Contains(err.Error(), "database is closed") {
						t.Errorf("错误文本泄漏驱动原始串: %v", err)
						return
					}
					continue
				}
				t.Errorf("并发操作返回非哨兵错误: %v", err)
				return
			}
		}(g)
	}
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Close panic: %v", r)
				}
			}()
			if err := p.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	wg.Wait()

	if _, _, err := p.Get(ctx, "crb:0"); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("关闭后应稳定 ErrClosed, got %v", err)
	}
}
