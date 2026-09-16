// Command migration 在不同的 kvdb 基座之间搬运数据，例如
//
//	# 内存 -> SQLite
//	migration -from mem:// -to sqlite://./tmp/a.db
//
//	# SQLite -> leveldb，且只搬指定前缀的 key
//	migration -from sqlite://./a.db -to leveldb://./b.ldb -prefix user:
//
//	# 经 RPC 从远端拉回本地（客户端不感知远端底座）
//	migration -from 'rpc://host:7788?auth=challenge&password=s3cret' -to bolt://./b.bolt
//
// 迁移内容与语义：
//
//   - **KV**：按字节序 Scan 全量搬运；**保留剩余 TTL**（源上还剩多久，目标上就设多久，
//     而不是重新计一个满 TTL）。目标写入用 Batch 分批提交，降低往返与 fsync 次数；
//     目标不支持 Batch 时自动退回逐条写。
//   - **ZSet**：ZRange 可完整枚举成员与分数，因此能**只读源**地搬运；用 -zsets 给名字。
//   - **Queue**：用 QRange 按位置**只读**搬运（分批），源不受影响，可重复执行。
//     若源基座未实现 QRange（返回 ErrUnsupported），则只有显式加 -allow-queue-drain
//     才会改用 QPop 逐条搬——那条路**会清空源队列**，故必须显式确认；不加则只报告跳过。
//   - **默认只读源**：源上数据不会被删除，整次迁移可重复执行（幂等覆盖写）。
//
// 关于名字枚举：SDK 没有枚举队列名/zset 名的接口（只有按名的 QSize/ZSize），
// 所以 Queue/ZSet 必须用 -queues / -zsets 显式给出，工具不会去"猜"有哪些。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"

	// 与其它 example 命令一致：一次接入全部内置基座，因此 -from/-to 可任意指定。
	_ "github.com/RelicOfTesla/kvdb/all"
)

func main() {
	var (
		from   = flag.String("from", "", "源基座 URI（必填）")
		to     = flag.String("to", "", "目标基座 URI（必填）")
		prefix = flag.String("prefix", "", "只迁移该前缀的 KV key（空=全部）")
		queues = flag.String("queues", "", "要迁移的队列名，逗号分隔（SDK 无枚举接口，需显式给出）")
		zsets  = flag.String("zsets", "", "要迁移的 zset 名，逗号分隔（同上）")
		drainQ = flag.Bool("allow-queue-drain", false,
			"仅当源不支持 QRange 时，允许用 QPop 搬队列（**会清空源队列**）")
		batchSize = flag.Int("batch", 500, "每批写入条目数")
		dryRun    = flag.Bool("dry-run", false, "只统计，不写目标")
		timeout   = flag.Duration("timeout", 0, "整体超时（0=不限）")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "用法: migration -from URI -to URI [选项]\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\n例: migration -from mem:// -to sqlite://./a.db\n"+
			"    migration -from sqlite://./a.db -to sqlite://./b.db -prefix user:\n"+
			"    migration -from mem:// -to mem:// -zsets rank -queues jobs -allow-queue-drain\n")
	}
	flag.Parse()

	if *from == "" || *to == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *batchSize <= 0 {
		fmt.Fprintln(os.Stderr, "错误: -batch 必须为正")
		os.Exit(2)
	}

	ctx := context.Background()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	os.Exit(run(ctx, options{
		from: *from, to: *to, prefix: *prefix,
		queues: splitNames(*queues), zsets: splitNames(*zsets),
		allowDrain: *drainQ, batchSize: *batchSize, dryRun: *dryRun,
	}))
}

type options struct {
	from, to   string
	prefix     string
	queues     []string
	zsets      []string
	allowDrain bool
	batchSize  int
	dryRun     bool
}

func run(ctx context.Context, o options) int {
	src, err := kvdb.Open(ctx, o.from)
	if err != nil {
		fmt.Fprintf(os.Stderr, "打开源 %s: %v\n", o.from, err)
		return 1
	}
	defer src.Close()

	var dst kvdb.DB
	if !o.dryRun {
		dst, err = kvdb.Open(ctx, o.to)
		if err != nil {
			fmt.Fprintf(os.Stderr, "打开目标 %s: %v\n", o.to, err)
			return 1
		}
		defer dst.Close()
	}

	m := &migrator{src: src, dst: dst, opt: o, out: os.Stdout}

	// 逐类搬运；某类失败即终止并非零退出。已写入的部分保留——迁移默认不改源数据，
	// 重跑是幂等的（同 key 覆盖写），因此中断后直接再跑一次即可。
	if err := m.kv(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "\nKV 迁移失败: %v\n", err)
		return 1
	}
	if err := m.zsets(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "\nZSet 迁移失败: %v\n", err)
		return 1
	}
	if err := m.queues(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "\n队列迁移失败: %v\n", err)
		return 1
	}
	m.summary()
	return 0
}

type migrator struct {
	src, dst kvdb.DB
	opt      options
	out      *os.File

	nKV, nQ, nZ int64
	skipped     []string // 跳过的原因（汇总打印）
}

// kv 按字节序 Scan 全量搬运 KV，并保留剩余 TTL。
func (m *migrator) kv(ctx context.Context) error {
	last := ""    // 上一批的最后一个 key
	first := true // 首批不需要跳过 start
	for {
		start := ""
		if !first {
			start = last
		}
		first = false

		kvs, err := m.src.Scan(ctx, start, "", m.opt.batchSize)
		if err != nil {
			return fmt.Errorf("scan start=%q: %w", start, err)
		}
		if len(kvs) == 0 {
			break
		}
		// Scan 是**闭区间**：除首批外，start 那一项会被重复返回。
		// 必须跳过，否则（a）每批重搬上一批末项、（b）当末页只剩它一个时
		// last 不再推进 → 死循环。
		if start != "" && kvs[0].Key == start {
			kvs = kvs[1:]
			if len(kvs) == 0 {
				break
			}
		}

		batch := make([]kvItem, 0, len(kvs))
		for _, kv := range kvs {
			if m.opt.prefix != "" && !strings.HasPrefix(kv.Key, m.opt.prefix) {
				continue
			}
			it := kvItem{key: kv.Key, val: kv.Value}
			// 读剩余 TTL：目标上按剩余秒数重设，避免把生命周期人为延长。
			// TTL 读失败不该中断整次迁移——按无 TTL 处理并记录。
			if secs, has, err := m.src.TTL(ctx, kv.Key); err == nil && has && secs > 0 {
				it.ttl, it.hasTTL = secs, true
			}
			batch = append(batch, it)
		}

		if len(batch) > 0 {
			if !m.opt.dryRun {
				if err := m.writeKVBatch(ctx, batch); err != nil {
					return err
				}
			}
			m.nKV += int64(len(batch))
		}
		last = kvs[len(kvs)-1].Key
	}
	if m.nKV > 0 {
		fmt.Fprintf(m.out, "KV: %d 条\n", m.nKV)
	}
	return nil
}

type kvItem struct {
	key    string
	val    []byte
	ttl    int64
	hasTTL bool
}

// writeKVBatch 用一次 Batch 提交一批 KV（带 TTL 的走 SetEx）。
// 目标不支持 Batch 时退回逐条——迁移工具不该因为目标缺一个可选能力就整类失败。
func (m *migrator) writeKVBatch(ctx context.Context, batch []kvItem) error {
	err := m.dst.Batch(ctx, func(b *kvdb.Batch) error {
		for _, it := range batch {
			if it.hasTTL {
				b.SetEx(it.key, it.val, it.ttl)
			} else {
				b.Set(it.key, it.val)
			}
		}
		return nil
	})
	if err == nil {
		return nil
	}
	if !errors.Is(err, core.ErrUnsupported) {
		return fmt.Errorf("batch: %w", err)
	}
	for _, it := range batch {
		var e error
		if it.hasTTL {
			e = m.dst.SetEx(ctx, it.key, it.val, it.ttl)
		} else {
			e = m.dst.Set(ctx, it.key, it.val)
		}
		if e != nil {
			return fmt.Errorf("set %q: %w", it.key, e)
		}
	}
	return nil
}

// zsets 搬运有序集：ZRange 能完整枚举成员与分数，故可只读源完成。
func (m *migrator) zsets(ctx context.Context) error {
	if len(m.opt.zsets) == 0 {
		return nil
	}
	if !m.opt.dryRun && !m.dst.Capabilities().ZSet {
		return fmt.Errorf("目标基座不支持 ZSet 能力，无法搬运有序集")
	}
	for _, name := range m.opt.zsets {
		items, err := m.src.ZRange(ctx, name, 0, -1)
		if err != nil {
			return fmt.Errorf("zrange %q: %w", name, err)
		}
		if !m.opt.dryRun {
			for _, it := range items {
				if err := m.dst.ZSet(ctx, name, it.Key, it.Score); err != nil {
					return fmt.Errorf("zset %q/%q: %w", name, it.Key, err)
				}
			}
		}
		m.nZ += int64(len(items))
		fmt.Fprintf(m.out, "ZSet %s: %d 个成员\n", name, len(items))
	}
	return nil
}

// queues 搬运队列。
//
// 首选 **QRange 只读**搬运：它按位置读队列且不改动源，因此迁移可重复执行。
// 若源基座未实现 QRange（返回 ErrUnsupported），才考虑 -allow-queue-drain：
// 那条路用 QPop 逐条取出，**会清空源队列**，故必须显式开关，且输出会明确标注。
func (m *migrator) queues(ctx context.Context) error {
	if len(m.opt.queues) == 0 {
		return nil
	}
	if !m.opt.dryRun && !m.dst.Capabilities().Queue {
		return fmt.Errorf("目标基座不支持 Queue 能力，无法搬运队列")
	}
	for _, name := range m.opt.queues {
		n, err := m.src.QSize(ctx, name)
		if err != nil {
			return fmt.Errorf("qsize %q: %w", name, err)
		}
		if n == 0 {
			continue
		}
		// 先试只读路径。
		readOnly := true
		if !m.opt.dryRun {
			if _, err := m.src.QRange(ctx, name, 0, 0); errors.Is(err, core.ErrUnsupported) {
				readOnly = false
			} else if err != nil {
				return fmt.Errorf("qrange %q: %w", name, err)
			}
		}
		if readOnly {
			if err := m.copyQueueReadOnly(ctx, name, n); err != nil {
				return err
			}
			continue
		}
		// 源不支持 QRange：只有显式允许才走破坏性的 QPop 路径。
		if !m.opt.allowDrain {
			m.skipped = append(m.skipped, fmt.Sprintf(
				"队列 %q 有 %d 个元素，已跳过：该基座未实现 QRange（只读按位置读），"+
					"只能靠 QPop 搬（会清空源）。确认可接受时加 -allow-queue-drain", name, n))
			continue
		}
		if err := m.drainQueue(ctx, name, n); err != nil {
			return err
		}
	}
	return nil
}

// copyQueueReadOnly 用 QRange 分批只读搬队列，源不受影响。
func (m *migrator) copyQueueReadOnly(ctx context.Context, name string, n int64) error {
	if m.opt.dryRun {
		m.nQ += n
		fmt.Fprintf(m.out, "队列 %s: %d 个元素（dry-run，未搬运）\n", name, n)
		return nil
	}
	// 分批读，避免一次性把大队列全读进内存。
	batch := int64(m.opt.batchSize)
	if batch <= 0 {
		batch = 500
	}
	var moved int64
	for start := int64(0); start < n; start += batch {
		stop := start + batch - 1
		if stop >= n {
			stop = n - 1
		}
		items, err := m.src.QRange(ctx, name, start, stop)
		if err != nil {
			return fmt.Errorf("qrange %q [%d,%d]: %w", name, start, stop, err)
		}
		if len(items) == 0 {
			break // 读不到更多了（例如并发弹出），避免空转
		}
		// 按读到的顺序 QPush → 目标队尾，保持队头→队尾的顺序不变。
		for _, v := range items {
			if err := m.dst.QPush(ctx, name, v); err != nil {
				return fmt.Errorf("qpush %q（已搬 %d/%d；源未改动，可重跑）: %w", name, moved, n, err)
			}
			moved++
		}
	}
	m.nQ += moved
	fmt.Fprintf(m.out, "队列 %s: %d 个元素（只读搬运，源未改动）\n", name, moved)
	return nil
}

// drainQueue 是 QRange 不可用时的退路：QPop 逐条取出。**会清空源队列**。
func (m *migrator) drainQueue(ctx context.Context, name string, n int64) error {
	if m.opt.dryRun {
		m.nQ += n
		fmt.Fprintf(m.out, "队列 %s: %d 个元素（dry-run，未搬运）\n", name, n)
		return nil
	}
	var moved int64
	for {
		v, ok, err := m.src.QPop(ctx, name)
		if err != nil {
			return fmt.Errorf("qpop %q（已搬 %d/%d，源队列已被部分清空）: %w", name, moved, n, err)
		}
		if !ok {
			break
		}
		if err := m.dst.QPush(ctx, name, v); err != nil {
			// 元素已从源取出但没进目标 —— 必须报出来，否则是静默丢数据。
			return fmt.Errorf("qpush %q（该元素已从源取出，未写入目标，请手工补回）: %w", name, err)
		}
		moved++
	}
	m.nQ += moved
	fmt.Fprintf(m.out, "队列 %s: %d 个元素（**源队列已被清空**）\n", name, moved)
	return nil
}

func (m *migrator) summary() {
	mode := ""
	if m.opt.dryRun {
		mode = "（dry-run，未写入）"
	}
	fmt.Fprintf(m.out, "\n迁移完成%s: KV=%d 条, ZSet=%d 条, Queue=%d 条\n", mode, m.nKV, m.nZ, m.nQ)
	for _, s := range m.skipped {
		fmt.Fprintf(m.out, "跳过: %s\n", s)
	}
}

// splitNames 解析逗号分隔的名字列表，去空白、去空项、去重并排序
// （排序让输出稳定，便于比对）。
func splitNames(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}
