package leveldb_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/RelicOfTesla/kvdb/leveldb"
)

// TestShardLockCorrectness 钉住分片锁设计的两条前提：
//
//  1. 读路径不取分片锁，也**读不到撕裂状态**——因为写的提交是单次原子
//     Write(batch)，读者只会看到旧值或新值；
//  2. 同 key 并发 Incr 不丢更（等价于串行），不同 key 各自独立计数。
//
// 这两条也是"分片锁用 Mutex 而非 RWMutex"的依据：锁内必定含写操作，没有 RLock
// 语义可用；读侧则完全不参与这把锁（见 Provider.keyMutex 注释）。
func TestShardLockCorrectness(t *testing.T) {
	ctx := context.Background()
	// 默认即不 fsync：本用例只验证锁语义（撕裂/丢更），不需要逐提交 fsync。
	p, err := leveldb.Open(ctx, t.TempDir()+"/db", leveldb.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// 先播种，使并发读者从"键存在"的状态开始（否则读不到是正常语义，不是缺陷）。
	if _, err := p.Incr(ctx, "hot", 0); err != nil {
		t.Fatal(err)
	}

	const writers, per = 4, 50
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 2; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v, ok, err := p.Get(ctx, "hot")
				if err != nil {
					t.Errorf("并发读: %v", err)
					return
				}
				if !ok {
					t.Error("并发读: 键不应消失")
					return
				}
				if _, perr := strconv.ParseInt(string(v), 10, 64); perr != nil {
					t.Errorf("读到非完整值（疑似撕裂）: %q", v)
					return
				}
			}
		}()
	}

	// 同 key 并发累加：终值必须精确等于总次数。
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				if _, err := p.Incr(ctx, "hot", 1); err != nil {
					t.Errorf("Incr: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	readers.Wait()

	if v, ok, err := p.Get(ctx, "hot"); err != nil || !ok || string(v) != strconv.Itoa(writers*per) {
		t.Fatalf("同 key 并发累加丢更: got %q ok=%v err=%v, want %d", v, ok, err, writers*per)
	}

	// 多 key 并发：各写者独占一个 key，互不干扰。
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := "multi:" + strconv.Itoa(id)
			for j := 0; j < per; j++ {
				if _, err := p.Incr(ctx, key, 1); err != nil {
					t.Errorf("多key Incr: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < writers; i++ {
		v, ok, err := p.Get(ctx, "multi:"+strconv.Itoa(i))
		if err != nil || !ok {
			t.Fatalf("multi:%d 读取失败: ok=%v err=%v", i, ok, err)
		}
		if string(v) != strconv.Itoa(per) {
			t.Fatalf("multi:%d = %q, want %d", i, v, per)
		}
	}
}
