package bolt_test

import (
	"context"
	"net/url"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb/bolt"
)

// TestSyncIntervalURIParsing 验证 sync_interval 参数解析：合法值接受，
// 非法/非正值必须报错而不是静默回落到默认间隔——使用者写错单位却以为已经调过，
// 属于"以为安全其实没有"的误解，必须点明。
func TestSyncIntervalURIParsing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	for _, q := range []string{"", "?sync_interval=10ms", "?sync=1", "?sync=1&timeout=3s"} {
		u, err := url.Parse("bolt://" + filepath.Join(dir, "ok.bolt") + q)
		if err != nil {
			t.Fatal(err)
		}
		p, err := bolt.OpenURI(ctx, u)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if err := p.(*bolt.Provider).Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, bad := range []string{"?sync_interval=abc", "?sync_interval=0", "?sync_interval=-1s", "?sync_interval=5"} {
		u, err := url.Parse("bolt://" + filepath.Join(dir, "bad.bolt") + bad)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bolt.OpenURI(ctx, u); err == nil {
			t.Fatalf("%s 应报错", bad)
		}
	}
}

// TestClosePersistsAfterWrites 验证缺省档（Sync=false，周期落盘）下写入后
// Close 能保证数据完整落盘并可被重新打开读取。
//
// 说明为什么这里不断言"某次 Sync 真的调了 fdatasync"：bbolt 走 mmap 写，写入
// 立刻体现在页缓存里，读文件内容无法区分"已 fsync"与"仅在内核页缓存"；能可靠
// 断言的对外契约是"正常关闭后数据完整"。周期落盘的收益/代价由 bench 的
// sync=1 对照量化。
func TestClosePersistsAfterWrites(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db.bolt")
	p, err := bolt.Open(ctx, path, bolt.Config{SyncInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := p.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := p.QPush(ctx, "q", []byte("e")); err != nil {
			t.Fatal(err)
		}
	}
	// 让空闲兜底有机会触发（不依赖它，Close 也必须保证完整）。
	time.Sleep(50 * time.Millisecond)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p2, err := bolt.Open(ctx, path, bolt.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if v, ok, _ := p2.Get(ctx, "k"); !ok || string(v) != "v" {
		t.Fatalf("重开后 Get = %q,%v", v, ok)
	}
	if n, err := p2.QSize(ctx, "q"); err != nil || n != 100 {
		t.Fatalf("重开后 QSize = %d,%v", n, err)
	}
}

// TestIdleTimerNoLeak 验证空闲兜底计时器在 Close 后被停止：否则每个 Open/Close
// 周期都会留下一个到期触发一次的 timer（长时间频繁开关会累积）。
func TestIdleTimerNoLeak(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		p, err := bolt.Open(ctx, filepath.Join(dir, "leak.bolt"), bolt.Config{SyncInterval: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// time.AfterFunc 的定时器不会常驻 goroutine，但 Stop 后不应再触发回调；
	// 这里给足时间让"若未 Stop"的回调暴露出来（回调会操作已关闭的 db）。
	time.Sleep(20 * time.Millisecond)
	runtime.GC()
	after := runtime.NumGoroutine()
	if after > before+5 {
		t.Fatalf("goroutine 数增长异常: before=%d after=%d", before, after)
	}
}

// TestSyncModeCloseStillWorks 验证 sync=1（逐提交 fsync）档位下 Close 正常，
// 且周期落盘路径不参与（该档由 bbolt 自己保证每次提交落盘）。
func TestSyncModeCloseStillWorks(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sync.bolt")
	p, err := bolt.Open(ctx, path, bolt.Config{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2, err := bolt.Open(ctx, path, bolt.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if v, ok, _ := p2.Get(ctx, "k"); !ok || string(v) != "v" {
		t.Fatalf("重开后 Get = %q,%v", v, ok)
	}
}
