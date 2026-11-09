package redis_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/RelicOfTesla/kvdb/redis"
)

// 本门禁钉住 Scan 的**代价**，而不只是产出：Scan 遍历键空间后必须**成批**取值，
// 不得对每条结果各发一次 GET。
//
// 背景（实测，见 UPSTREAM_VALUE.md）：旧的 `Scan` 在收集并排序键之后逐键 `GET`，
// 于是 `Scan(limit=N)` 的往返数 = N+1；Redis 有多键取值命令（MGET），这个 O(N)
// 往返没有任何语义上的必要。产出在修复前后逐条相同，因此**语义用例抓不到它**
// ——必须断言命令数（确定性），不能只断言返回值。
//
// 判据用服务端 `INFO commandstats` 的 `get` 调用数差值：
//   - 记录数 20 → 600（30×）时，`get` 数不得随之线性增长；
//   - 允许极小常数（0 附近），分页/批大小只应增加 `mget`，绝不增加 `get`。
//
// R5 说明：命令计数是**服务端全局**的。本用例要求本模块独占的库号，并在测量前
// 做空窗校准；若实例上确有第三方并发，本门禁只会**假失败**（计数偏大），不会
// 假通过。需要显式设置 KVDB_TEST_REDIS_ADDR（与 TestRealRedis 同一开关）。

// scanGateAddr 返回真实 Redis 地址与库号。缺地址时 SKIP（与 TestRealRedis 同
// 口径）；库号恒不为 0——用例会对自己的库发 FlushDB，DB0 可能是他人数据。
func scanGateAddr(t *testing.T) (string, int) {
	t.Helper()
	addr := os.Getenv("KVDB_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("未设置 KVDB_TEST_REDIS_ADDR，跳过真实 Redis 门禁")
	}
	db := 15
	if v := os.Getenv("KVDB_TEST_REDIS_DB"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("KVDB_TEST_REDIS_DB=%q 不是整数: %v", v, err)
		}
		db = n
	}
	if db == 0 {
		t.Fatalf("拒绝在 DB0 上运行：FlushDB(DB0) 可能清掉同实例的真实数据")
	}
	return addr, db
}

// scanGateFlushDB 只清本用例自己的库号。
func scanGateFlushDB(t *testing.T, addr string, db int) {
	t.Helper()
	cli := goredis.NewClient(&goredis.Options{Addr: addr, DB: db})
	defer cli.Close()
	if err := cli.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("FlushDB(db=%d): %v", db, err)
	}
}

func TestScanDoesNotPointQueryPerRecord(t *testing.T) {
	addr, db := scanGateAddr(t)
	ctx := context.Background()
	scanGateFlushDB(t, addr, db)
	t.Cleanup(func() { scanGateFlushDB(t, addr, db) })

	p, err := redis.Open(ctx, redis.Config{Addr: addr, DB: db, KeyPrefix: "scangate:"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// 600 > 单批上限（512），既覆盖跨批取值，又让"小页 vs 全量"相差 30×。
	const n = 600
	for i := 0; i < n; i++ {
		key := "k" + pad3(i)
		if err := p.SetEx(ctx, key, []byte("v:"+key), 300); err != nil {
			t.Fatal(err)
		}
	}

	cli := goredis.NewClient(&goredis.Options{Addr: addr, DB: db})
	defer cli.Close()

	// 空窗校准：若这段窗口里已有 GET 活动，计数增量不可归因（R5），跳过而不是
	// 把别人的流量算到 Scan 头上。
	base := commandCalls(t, cli, "get")
	time.Sleep(100 * time.Millisecond)
	if got := commandCalls(t, cli, "get"); got != base {
		t.Skipf("实例在 100ms 空窗内有并发 GET（%d→%d），命令计数不可归因", base, got)
	}

	// 小页：旧实现 20 条 GET，新实现 0 条。
	start := commandCalls(t, cli, "get")
	small, err := p.Scan(ctx, "", "", 20)
	if err != nil {
		t.Fatal(err)
	}
	getsSmall := commandCalls(t, cli, "get") - start
	if len(small) != 20 {
		t.Fatalf("Scan(limit=20) 返回 %d 条，want 20", len(small))
	}
	if getsSmall > 4 {
		t.Errorf("Scan(limit=20) 发了 %d 条 GET：值是按记录逐条点查取回的，应改用批量取值（MGET）", getsSmall)
	}

	// 全量：记录数 30×，GET 数不得跟着涨（旧实现 600 条）。
	start = commandCalls(t, cli, "get")
	all, err := p.Scan(ctx, "", "", n)
	if err != nil {
		t.Fatal(err)
	}
	getsAll := commandCalls(t, cli, "get") - start
	t.Logf("GET 数：limit=20 → %d；limit=%d → %d（记录数 30×，GET 不随记录数增长）", getsSmall, n, getsAll)
	if getsAll > getsSmall+2 {
		t.Errorf("记录数 20→%d 时 GET 数从 %d 涨到 %d —— 随记录数线性增长，说明值仍是逐条点查取回", n, getsSmall, getsAll)
	}

	// 语义断言：批量取值不得改变产出（闭区间、升序、键值对应、跨批不错位）。
	if len(all) != n {
		t.Fatalf("Scan(limit=%d) 返回 %d 条，want %d", n, len(all), n)
	}
	for i, kv := range all {
		wantKey := "k" + pad3(i)
		if kv.Key != wantKey || string(kv.Value) != "v:"+wantKey {
			t.Fatalf("Scan 第 %d 条 = {%q,%q}，want {%q,%q}（批量取值错位/乱序）",
				i, kv.Key, kv.Value, wantKey, "v:"+wantKey)
		}
	}
}

// pad3 把 0..599 格式化成三位十进制（键序 = 字典序 = 数值序）。
func pad3(i int) string {
	s := strconv.Itoa(i)
	for len(s) < 3 {
		s = "0" + s
	}
	return s
}

// commandCalls 读取服务端 `INFO commandstats` 中某个命令的累计调用数。
func commandCalls(t *testing.T, cli *goredis.Client, cmd string) int {
	t.Helper()
	info, err := cli.Info(context.Background(), "commandstats").Result()
	if err != nil {
		t.Fatalf("INFO commandstats: %v", err)
	}
	prefix := "cmdstat_" + cmd + ":"
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		for _, field := range strings.Split(strings.TrimPrefix(line, prefix), ",") {
			if v, ok := strings.CutPrefix(field, "calls="); ok {
				n, err := strconv.Atoi(v)
				if err != nil {
					t.Fatalf("解析 %q 的 calls 失败: %v", line, err)
				}
				return n
			}
		}
	}
	return 0
}
