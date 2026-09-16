package jsonl

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ---- 缺省周期档 ----

// TestDefaultPeriodicFlush 验证缺省档（周期 500ms）的核心承诺：**没有后续写入
// 也会落盘**。写入后不 Close、不再写，靠 flush 周期把数据交给 OS。
func TestDefaultPeriodicFlush(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "periodic.jsonl")
	p, err := Open(ctx, path, Config{FlushInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	waitForContent(t, path, 2*time.Second, "周期 flush 未发生：无后续写入时数据始终未交给 OS")
}

// TestPeriodicFlushUnderContinuousWrites 验证周期是**真周期**而非"空闲才触发"：
// 持续写入期间也必须按期落盘，否则"写完最后一批就再也不落盘"会重现。
func TestPeriodicFlushUnderContinuousWrites(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "busy.jsonl")
	p, err := Open(ctx, path, Config{FlushInterval: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	stop := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(stop) {
		if err := p.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	if data, err := os.ReadFile(path); err != nil || len(data) == 0 {
		t.Fatalf("持续写入 300ms（周期 30ms）后文件为空: len=%d err=%v", len(data), err)
	}
}

// TestFlushIntervalControlsRate 验证 flush 频率由 FlushInterval 控制。
func TestFlushIntervalControlsRate(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "long.jsonl")
	p, err := Open(ctx, path, Config{FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
		t.Fatal("flush 间隔为 1h 时不应在 200ms 内落盘")
	}
}

// TestSyncTickFlushesBeforeFsync 回归：周期 fsync 之前必须先 flush。
//
// fsync 只作用于 fd 上"已写出去"的字节；数据还在 bufio 缓冲里时 Sync 刷不到它，
// 这次 fsync 就等于白做。症状极隐蔽（fsync 照常发生，只是同步了个空），必须钉住。
// 构造：flush 周期远大于 fsync 周期——若不先 flush，数据会一直躺在缓冲里。
func TestSyncTickFlushesBeforeFsync(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "syncflush.jsonl")
	p, err := Open(ctx, path, Config{
		FlushInterval: time.Hour,             // 周期 flush 基本不发生
		SyncInterval:  20 * time.Millisecond, // fsync 频繁发生
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	waitForContent(t, path, 2*time.Second, "周期 fsync 未先把缓冲交给 OS（fsync 空刷）")
}

// ---- 逐操作档 ----

// TestEachFlushMakesWriteFailuresImmediate 验证 each_flush=1 下写失败在**当次**
// 操作就上报，且失败的写入不进内存。
func TestEachFlushMakesWriteFailuresImmediate(t *testing.T) {
	ctx := context.Background()
	p, err := Open(ctx, filepath.Join(t.TempDir(), "f.jsonl"), Config{EachFlush: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Set(ctx, "k", []byte("v0")); err != nil {
		t.Fatal(err)
	}
	p.file.Close() // 破坏日志
	if err := p.Set(ctx, "k", []byte("v1")); err == nil {
		t.Fatal("each_flush=1 下日志不可写时 Set 应在当次返回错误")
	}
	if v, ok, _ := p.Get(ctx, "k"); !ok || string(v) != "v0" {
		t.Fatalf("写失败后内存应保持 v0, got %q,%v", v, ok)
	}
}

// TestEachFlushWritesImmediately 验证 each_flush=1 每次写都交给 OS（不等周期）。
func TestEachFlushWritesImmediately(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "each.jsonl")
	// 周期给很大：each_flush 应完全忽略它
	p, err := Open(ctx, path, Config{EachFlush: true, FlushInterval: time.Hour, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		t.Fatalf("each_flush=1 写入后应立即落盘: len=%d err=%v", len(data), err)
	}
}

// TestSyncModeWritesImmediately 验证 sync=1 每次写都 flush+fsync。
func TestSyncModeWritesImmediately(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sync.jsonl")
	p, err := Open(ctx, path, Config{Sync: true, FlushInterval: time.Hour, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		t.Fatalf("sync=1 写入后应立即落盘: len=%d err=%v", len(data), err)
	}
}

// ---- 失败即停写（bufio 粘性错误）----

// TestWriteFailurePoisonsProvider 验证写失败后 Provider 立刻停写。
//
// bufio.Writer 的错误是粘性的且没有公开 API 清除：一旦出错，缓冲里残留的字节
// 再也不会被写出去，后续 Write 也只会拿到同一个陈旧错误。此时若继续"报成功"，
// 调用方会以为数据已持久化。因此实现选择 fail-stop：任何 flush/write 失败后，
// 后续写操作一律返回错误，且 Close 也把该错误报出来。
func TestWriteFailurePoisonsProvider(t *testing.T) {
	ctx := context.Background()
	p, err := Open(ctx, filepath.Join(t.TempDir(), "poison.jsonl"), Config{
		FlushInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Set(ctx, "k", []byte("v0")); err != nil {
		t.Fatal(err)
	}
	p.file.Close() // 破坏日志

	// 等到周期 flush 暴露错误
	var failed bool
	for i := 0; i < 200 && !failed; i++ {
		if err := p.Set(ctx, "k", []byte("v1")); err != nil {
			failed = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !failed {
		t.Fatal("日志不可写时始终未报错（写失败被吞）")
	}
	// 停写：之后所有写都必须失败，且不得改动内存
	before, _, _ := p.Get(ctx, "k")
	if err := p.Set(ctx, "k", []byte("v2")); err == nil {
		t.Fatal("日志已失败后 Set 仍返回成功（未 fail-stop）")
	}
	after, _, _ := p.Get(ctx, "k")
	if string(before) != string(after) {
		t.Fatalf("失败后的写入进入了内存: %q -> %q", before, after)
	}
}

// TestWriteFailureReportedOnClose 验证失败在 Close 时必定被报出——
// 不能因为失败发生在后台周期回调里就静默丢失。
func TestWriteFailureReportedOnClose(t *testing.T) {
	ctx := context.Background()
	p, err := Open(ctx, filepath.Join(t.TempDir(), "closefail.jsonl"), Config{
		FlushInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	p.file.Close() // 破坏日志：后续周期 flush 必然失败
	time.Sleep(100 * time.Millisecond)
	if err := p.Close(); err == nil {
		t.Fatal("后台落盘失败必须在 Close 报出，不能静默")
	}
}

// ---- 缺省值与参数解析 ----

// TestDefaultIntervals 钉住两个旋钮的缺省值：flush 500ms、fsync 1000ms。
// 这是缺省耐久等级的定义，改动它等于改变所有使用者的默认丢失边界。
func TestDefaultIntervals(t *testing.T) {
	if DefaultFlushInterval != 500*time.Millisecond {
		t.Fatalf("缺省 flush 间隔 = %v, want 500ms", DefaultFlushInterval)
	}
	if DefaultSyncInterval != time.Second {
		t.Fatalf("缺省 fsync 间隔 = %v, want 1s", DefaultSyncInterval)
	}
	p, err := Open(context.Background(), filepath.Join(t.TempDir(), "d.jsonl"), Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.sync || p.eachFlush {
		t.Fatal("Config{} 零值应是周期档（非 sync、非 each_flush）")
	}
	if p.flushInterval != DefaultFlushInterval || p.syncInterval != DefaultSyncInterval {
		t.Fatalf("缺省间隔 = %v/%v, want %v/%v",
			p.flushInterval, p.syncInterval, DefaultFlushInterval, DefaultSyncInterval)
	}
	if p.flushTimer == nil || p.syncTimer == nil {
		t.Fatal("周期档必须装两个计时器")
	}
}

// TestTimerConfiguration 验证三种档位下计时器的装配方式。
func TestTimerConfiguration(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		cfg       Config
		wantFlush bool
		wantSync  bool
	}{
		{"周期档", Config{}, true, true},
		{"each_flush", Config{EachFlush: true}, false, true},
		{"sync", Config{Sync: true, EachFlush: true}, false, false},
	} {
		p, err := Open(ctx, filepath.Join(t.TempDir(), tc.name+".jsonl"), tc.cfg)
		if err != nil {
			t.Fatal(err)
		}
		if (p.flushTimer != nil) != tc.wantFlush {
			t.Errorf("%s: flushTimer 存在=%v, want %v", tc.name, p.flushTimer != nil, tc.wantFlush)
		}
		if (p.syncTimer != nil) != tc.wantSync {
			t.Errorf("%s: syncTimer 存在=%v, want %v", tc.name, p.syncTimer != nil, tc.wantSync)
		}
		p.Close()
	}
}

// TestOpenURIParsing 验证四个参数的解析：合法值接受，非法/非正值必须报错
// 而不是静默回落到默认间隔。
func TestOpenURIParsing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	for _, q := range []string{
		"", "?sync=1", "?each_flush=1",
		"?flush_interval=10ms", "?sync_interval=10ms",
		"?each_flush=1&sync_interval=5s&flush_interval=5s",
		"?sync=1&each_flush=1&sync_interval=5s&flush_interval=5s",
	} {
		u, err := url.Parse("jsonl://" + filepath.Join(dir, "ok.jsonl") + q)
		if err != nil {
			t.Fatal(err)
		}
		p, err := OpenURI(ctx, u)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if err := p.(*Provider).Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, bad := range []string{
		"?flush_interval=abc", "?flush_interval=0", "?flush_interval=-1s",
		"?sync_interval=abc", "?sync_interval=0", "?sync_interval=-1s", "?sync_interval=5",
	} {
		u, err := url.Parse("jsonl://" + filepath.Join(dir, "bad.jsonl") + bad)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := OpenURI(ctx, u); err == nil {
			t.Fatalf("%s 应报错", bad)
		}
	}
}

// TestCloseFlushes 验证任何档位下 Close 后数据完整可回放。
func TestCloseFlushes(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "close.jsonl")
	p, err := Open(ctx, path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := p.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2, err := Open(ctx, path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if v, ok, _ := p2.Get(ctx, "k"); !ok || string(v) != "v" {
		t.Fatalf("Close 后重开 Get = %q,%v", v, ok)
	}
}

// waitForContent 轮询等待文件出现内容（不 Close、不再写）。
func waitForContent(t *testing.T, path string, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
