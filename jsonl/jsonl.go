// Package jsonl 提供 append-only JSONL 基座：每个写操作追加一行 JSON 记录，
// 打开时回放日志到内存（实际复用 kvdb/mem 基座提供读取语义），崩溃后重新回放
// 即恢复状态。适合单进程内嵌使用；日志会增长，需定期 Compact 压缩（重写为
// 当前状态的等价操作序列）。
//
// 记录中的过期时间以绝对 unix 秒存储，回放时可确定性地还原原始 TTL，不因
// 停机时长缩短有效期。值按"合法 UTF-8 直接存原文，否则 base64"编码，便于
// 人工阅读日志。
package jsonl

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/mem"
)

// OpenURI 解析 jsonl://<path>?sync=1；路径支持 jsonl://./x、jsonl:///abs/x。
func OpenURI(ctx context.Context, u *url.URL) (core.KvProvider, error) {
	cfg := Config{Sync: u.Query().Get("sync") == "1"}
	return Open(ctx, pathFromURL(u), cfg)
}

// pathFromURL 归一化文件型路径：Host 为空表示绝对路径 jsonl:///abs/x。
func pathFromURL(u *url.URL) string {
	if u.Host == "" {
		return u.Path
	}
	return strings.TrimPrefix(u.Host+u.Path, "/")
}

var (
	_ core.FullProvider = (*Provider)(nil)
)

// Config 控制 JSONL 基座行为。
type Config struct {
	// Sync 为 true 时每个写操作后立即 fsync（断电安全，吞吐低）；
	// 默认 false：每个操作 flush 到 OS，仅 Close/Compact 时 fsync。
	Sync bool
}

// 日志记录的操作名。它们是**持久化格式的一部分**：写入侧与回放侧必须逐字一致，
// 用常量而非字面量，避免某一侧写错时不被编译器发现（表现为静默回放失败）。
const (
	opSet    = "set"
	opSetEx  = "setx"
	opDel    = "del"
	opIncr   = "incr"
	opExpire = "expire"
	opQPush  = "qpush"
	opQPop   = "qpop"
	opZSet   = "zset"
	opZDel   = "zdel"
	opZIncr  = "zincr"
	opBatch  = "batch"
)

// 缓冲大小：写缓冲统一走 writeBufSize（Open / Compact 临时文件 / Compact 重开
// 三处必须一致，否则同一日志的刷写粒度会随路径变化）；读缓冲用于回放。
const (
	writeBufSize = 32 * 1024
	readBufSize  = 64 * 1024
)

// MaxRecordBytes 是单条日志记录（含批量记录）的字节上限。写入侧会在此处拒绝
// 超限值（而不是"写成功、重启时打不开"），回放侧使用同一上限。
// 非 UTF-8 值走 base64，膨胀约 4/3，因此二进制值的可用上限约为该值的 3/4。
const MaxRecordBytes = 64 << 20

// op 是日志的一行记录；字段为各数据结构所需的最小集合。
type op struct {
	Op  string `json:"op"`            // set|setx|del|incr|expire|qpush|qpop|zset|zdel|zincr|batch
	K   string `json:"k,omitempty"`   // kv key / 队列名 / zset 名
	M   string `json:"m,omitempty"`   // zset 成员
	V   string `json:"v,omitempty"`   // set / qpush 的值（文本或 base64）
	B64 bool   `json:"b64,omitempty"` // V 为 base64 编码
	KB  bool   `json:"kb,omitempty"`  // K 为 base64 编码（非 UTF-8 key，见 encStr）
	MB  bool   `json:"mb,omitempty"`  // M 为 base64 编码（非 UTF-8 成员）
	D   int64  `json:"d,omitempty"`   // incr / zincr 增量
	S   int64  `json:"s,omitempty"`   // zset 分数
	At  int64  `json:"at,omitempty"`  // expire 绝对过期时间（unix 秒，必然 >0）
	F   bool   `json:"f,omitempty"`   // qpush 到队头 / qpop 从队尾
	// B 是批量记录的子操作（Op=="batch" 时非空）。整批只占一行，
	// 因此崩溃只会留下"完整前缀行"或"半行"——不会出现提交半个批。
	B []op `json:"b,omitempty"`
}

// parseError 标记"这一行不是合法 JSON"（崩溃残留的半行），与"解析成功但应用失败"
// 区分开：只有前者可以在回放末尾被容忍。
type parseError struct {
	line int
	err  error
}

func (e *parseError) Error() string {
	return fmt.Sprintf("jsonl: replay line %d: %v", e.line, e.err)
}
func (e *parseError) Unwrap() error { return e.err }

// Provider 是 JSONL 基座。并发安全；ctx 仅用于接口一致。
//
// 关于 mu 的职责：它保护**日志写入流**（bw/file 的追加与 Compact 时的换名重开），
// 因此所有写操作必须串行——单条日志的顺序就是这份 WAL 的语义。它不保护读：
// 读操作只经 mem（其自身有锁）并读原子的 closed，既不碰 file/bw，也不参与换名，
// 所以读路径不取 mu，读写可以真正并行。若把读也纳入这把锁（哪怕用 RLock），
// 一个写者就会阻塞全部读者，而读侧本来不需要任何额外一致性保证。
type Provider struct {
	mu     sync.Mutex
	mem    *mem.Provider
	file   *os.File
	bw     *bufio.Writer
	path   string
	sync   bool
	closed atomic.Bool
}

// Open 打开（不存在则创建）日志文件并回放恢复状态。
func Open(ctx context.Context, path string, cfg Config) (*Provider, error) {
	_ = ctx
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("jsonl: open %s: %w", path, err)
	}
	p := &Provider{
		mem:  mem.New(),
		file: f,
		bw:   bufio.NewWriterSize(f, writeBufSize),
		path: path,
		sync: cfg.Sync,
	}
	if err := p.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return p, nil
}

// replay 打开时流式回放日志（内存峰值限单行，不缓存全文件）。
// 容错规则：最后一行无法解析视为崩溃残留的半行——**截断到最后有效行末**
// 再返回（否则 O_APPEND 追加的下一条记录会与残片合并成一行，下次打开
// 轻则日志变砖、重则静默吞掉已确认的写入）；若坏行之后还有完整行，
// 则说明日志损坏，返回错误；最后一行"解析成功但无法应用"（如未知 op、
// base64 损坏）属于日志损坏，必须上报而不是静默忽略。
// 实现采用"延迟一行"策略：读入新行时先应用上一行——上一行若坏且仍读到
// 后续行即为中间损坏；EOF 时上行的解析结果决定是否截断容忍。
func (p *Provider) replay() error {
	if _, err := p.file.Seek(0, 0); err != nil {
		return fmt.Errorf("jsonl: seek: %w", err)
	}
	rd := bufio.NewReaderSize(p.file, readBufSize)
	now := core.NowUnix()

	// readLine 以 '\n' 分隔读一行（含行尾换行符）；与 Scanner 的
	// ErrTooLong 语义等价，但能同时给出字节偏移供截断使用。
	readLine := func() ([]byte, error) {
		var acc []byte
		for {
			frag, ferr := rd.ReadSlice('\n')
			if ferr == bufio.ErrBufferFull {
				acc = append(acc, frag...)
				if len(acc) > MaxRecordBytes+1 {
					return nil, fmt.Errorf("jsonl: line exceeds %d bytes", MaxRecordBytes+1)
				}
				continue
			}
			if ferr != nil && ferr != io.EOF {
				return nil, ferr
			}
			if len(acc) == 0 {
				return frag, ferr // 常规路径；ferr==io.EOF 时 frag 为 EOF 前残段
			}
			acc = append(acc, frag...)
			if len(acc) > MaxRecordBytes+1 {
				return nil, fmt.Errorf("jsonl: line exceeds %d bytes", MaxRecordBytes+1)
			}
			return acc, ferr
		}
	}

	// deferred 是暂缓的一行及其起始偏移：读入下一行前应用它，
	// 以便区分"坏行是最后一行（截断容忍）"与"坏行在中间（报错）"。
	var deferred *string
	var deferredStart int64
	lineNo := 0
	pos := int64(0)
	terminated := true // 最后一行是否以换行符结束
	flush := func() error {
		if deferred == nil {
			return nil
		}
		line, no := *deferred, lineNo
		deferred = nil // 先取走：调用方据返回值判定，不依赖残留状态
		if line == "" {
			return nil
		}
		var rec op
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return &parseError{line: no, err: err}
		}
		return p.apply(rec, now)
	}
	for {
		lineStart := pos
		line, rerr := readLine()
		if len(line) == 0 && rerr == io.EOF {
			break // 干净 EOF：无更多内容
		}
		if rerr != nil && rerr != io.EOF {
			return fmt.Errorf("jsonl: read: %w", rerr)
		}
		pos += int64(len(line))
		terminated = len(line) > 0 && line[len(line)-1] == '\n'
		// 先应用上一行；坏行若在此报错说明它后面还有行（中间损坏）。
		if err := flush(); err != nil {
			return err
		}
		s := strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r")
		deferred, deferredStart = &s, lineStart
		lineNo++
		if rerr == io.EOF {
			break // 无换行符的最后残段：交给 EOF 分支判定
		}
	}
	// EOF：deferred 为最后一行。仅"JSON 解析失败"按崩溃残留容忍；
	// 应用失败（未知 op、base64 损坏等）说明日志本身损坏，必须上报。
	if err := flush(); err != nil {
		var pe *parseError
		if !errors.As(err, &pe) {
			return err
		}
		// 崩溃残留半行：截断到最后有效行末并落盘，修复文件帧结构。
		if err := p.file.Truncate(deferredStart); err != nil {
			return fmt.Errorf("jsonl: truncate torn tail: %w", err)
		}
		if err := p.file.Sync(); err != nil {
			return fmt.Errorf("jsonl: sync after truncate: %w", err)
		}
		return nil
	}
	// 最后一行解析成功但缺换行符（上次进程在完整记录写入中途断电）：
	// 记录已应用，补一个换行符修复帧结构，防止下次追加与之合并。
	if !terminated && deferred != nil && *deferred != "" {
		if _, err := p.file.Write([]byte{'\n'}); err != nil { // O_APPEND 追加到末尾
			return fmt.Errorf("jsonl: repair missing newline: %w", err)
		}
		if err := p.file.Sync(); err != nil {
			return fmt.Errorf("jsonl: sync after newline repair: %w", err)
		}
	}
	return nil
}

// apply 将一条记录作用到内存状态（与写路径顺序一致即可确定性还原）。
// K/M/V 一律先解码：base64 损坏按"解析成功但无法应用"上报（中断打开），
// 不静默替换成空值。
func (p *Provider) apply(rec op, now int64) error {
	key, err := dec(rec.K, rec.KB)
	if err != nil {
		return err
	}
	member, err := dec(rec.M, rec.MB)
	if err != nil {
		return err
	}
	value, err := dec(rec.V, rec.B64)
	if err != nil {
		return err
	}
	switch rec.Op {
	case opSet:
		return p.mem.Set(context.Background(), string(key), value)
	case opSetEx:
		if err := p.mem.Set(context.Background(), string(key), value); err != nil {
			return err
		}
		remain := rec.At - now
		if remain <= 0 {
			return p.mem.Del(context.Background(), string(key))
		}
		return p.mem.Expire(context.Background(), string(key), remain)
	case opDel:
		return p.mem.Del(context.Background(), string(key))
	case opIncr:
		_, err := p.mem.Incr(context.Background(), string(key), rec.D)
		return err
	case opExpire:
		remain := rec.At - now
		if remain <= 0 {
			return p.mem.Del(context.Background(), string(key))
		}
		return p.mem.Expire(context.Background(), string(key), remain)
	case opQPush:
		if rec.F {
			return p.mem.QPushFront(context.Background(), string(key), value)
		}
		return p.mem.QPush(context.Background(), string(key), value)
	case opQPop:
		if rec.F {
			_, _, err := p.mem.QPopBack(context.Background(), string(key))
			return err
		}
		_, _, err := p.mem.QPop(context.Background(), string(key))
		return err
	case opZSet:
		return p.mem.ZSet(context.Background(), string(key), string(member), rec.S)
	case opZDel:
		return p.mem.ZDel(context.Background(), string(key), string(member))
	case opZIncr:
		_, err := p.mem.ZIncr(context.Background(), string(key), string(member), rec.D)
		return err
	case opBatch:
		// 整批记录：逐条应用。写入侧已保证同一事务式地落在一行里，
		// 回放时不会出现"半个批"。
		for _, sub := range rec.B {
			if err := p.apply(sub, now); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown op %q", rec.Op)
	}
}

// appendOp 追加一条记录并落缓冲（Sync 模式下立即 fsync）。
func (p *Provider) appendOp(rec op) error {
	return p.appendOps([]op{rec})
}

// appendOps 把一批记录写入缓冲后只做一次 Flush（Sync 模式下一次 fsync）。
// 单条与批量共用此路径，保证"成功返回 = 已交给 OS"的语义一致。
// 每条记录写入前校验长度：超限值宁可在写入时报错，也不能写进去让下次
// Open 因回放令牌超限而失败（"写得进、打不开"）。
func (p *Provider) appendOps(recs []op) error {
	if len(recs) == 0 {
		return nil
	}
	var buf []byte
	for _, rec := range recs {
		b, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("jsonl: marshal: %w", err)
		}
		if len(b)+1 > MaxRecordBytes {
			return fmt.Errorf("jsonl: record too large: %d bytes (limit %d, non-UTF-8 values use base64 and grow ~4/3)",
				len(b), MaxRecordBytes)
		}
		buf = append(buf, b...)
		buf = append(buf, '\n')
	}
	if _, err := p.bw.Write(buf); err != nil {
		return err
	}
	if err := p.bw.Flush(); err != nil {
		return err
	}
	if p.sync {
		return p.file.Sync()
	}
	return nil
}

// ---- Batch ----

// ApplyBatch 实现 core.BatchProvider：整批写成**一行** batch 记录（一次 Flush，
// Sync 模式一次 fsync），成功后再按序应用到内存。写日志失败时内存不变，整批不生效。
//
// 为什么是一行而不是多行：多行记录在崩溃时可能只写下完整前缀，回放就会提交
// 半个批；单行记录要么完整、要么是残缺半行（回放末尾按崩溃残留丢弃），
// 因此"整批全有或全无"在崩溃语义下也成立。
// BatchComposed 恒为 true：本基座在同一把锁/同一事务内逐条应用批操作，
// 批内后续操作能看到前序效果（见 core.BatchComposedProvider）。
func (p *Provider) BatchComposed() bool { return true }

var _ core.BatchComposedProvider = (*Provider)(nil)

func (p *Provider) ApplyBatch(ctx context.Context, ops []core.BatchOp) error {
	if len(ops) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return core.ErrClosed
	}
	recs := make([]op, 0, len(ops))
	now := core.NowUnix()
	for _, o := range ops {
		rec, err := toRecord(o, now)
		if err != nil {
			return err
		}
		recs = append(recs, rec)
	}
	// 日志先行：失败则内存不变。整批序列化为一条记录（原子性所在）。
	if err := p.appendOps([]op{{Op: opBatch, B: recs}}); err != nil {
		return err
	}
	for _, rec := range recs {
		if err := p.apply(rec, now); err != nil {
			return err
		}
	}
	return nil
}

// toRecord 把契约批操作翻译成日志记录（与单条写路径共用同一记录格式，
// 回放逻辑因此完全一致）。批内均为无条件写，此处只需校验 TTL。
func toRecord(o core.BatchOp, now int64) (op, error) {
	switch o.Kind {
	case core.BatchSet:
		return encOp(op{Op: opSet}, o.Key, "", o.Value), nil
	case core.BatchSetEx:
		if o.TTL <= 0 {
			return op{}, core.ErrInvalidTTL
		}
		return encOp(op{Op: opSetEx, At: core.AddTTL(now, o.TTL)}, o.Key, "", o.Value), nil
	case core.BatchDel:
		return encOp(op{Op: opDel}, o.Key, "", nil), nil
	case core.BatchExpire:
		if o.TTL <= 0 {
			return op{}, core.ErrInvalidTTL
		}
		return encOp(op{Op: opExpire, At: core.AddTTL(now, o.TTL)}, o.Key, "", nil), nil
	case core.BatchQPush:
		return encOp(op{Op: opQPush}, o.Key, "", o.Value), nil
	case core.BatchQPushFront:
		return encOp(op{Op: opQPush, F: true}, o.Key, "", o.Value), nil
	case core.BatchZSet:
		return encOp(op{Op: opZSet, S: o.Score}, o.Key, o.Member, nil), nil
	case core.BatchZDel:
		return encOp(op{Op: opZDel}, o.Key, o.Member, nil), nil
	case core.BatchZIncr:
		return encOp(op{Op: opZIncr, D: o.Delta}, o.Key, o.Member, nil), nil
	default:
		return op{}, fmt.Errorf("jsonl: unknown batch op %d", o.Kind)
	}
}

// ---- KV ----

func (p *Provider) Set(ctx context.Context, key string, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return core.ErrClosed
	}
	// WAL 顺序：先写日志、再改内存。若写日志失败则内存不变，二者始终一致
	//（反序会出现"内存已改、日志缺失"，重启回放后状态回退）。
	if err := p.appendOp(encOp(op{Op: opSet}, key, "", value)); err != nil {
		return err
	}
	return p.mem.Set(ctx, key, value)
}

// SetEx 写入 value 并覆盖 TTL（对应 Redis SETEX / SSDB setx）；
// 日志记录绝对过期时间，回放可确定性还原。
func (p *Provider) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return core.ErrClosed
	}
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	if err := p.appendOp(encOp(op{Op: opSetEx, At: core.AddTTL(core.NowUnix(), ttl)}, key, "", value)); err != nil {
		return err
	}
	return p.mem.SetEx(ctx, key, value, ttl)
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if p.closed.Load() {
		return nil, false, core.ErrClosed
	}
	return p.mem.Get(ctx, key)
}

func (p *Provider) Del(ctx context.Context, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return core.ErrClosed
	}
	if err := p.appendOp(encOp(op{Op: opDel}, key, "", nil)); err != nil {
		return err
	}
	return p.mem.Del(ctx, key)
}

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	if p.closed.Load() {
		return false, core.ErrClosed
	}
	return p.mem.Exists(ctx, key)
}

func (p *Provider) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return 0, core.ErrClosed
	}
	// 先校验既有值可解析为整数：若直接写日志再应用，日志里会留下一条
	// 回放必然失败的记录（毒化日志）；校验通过后再写日志，应用阶段不会失败。
	if cur, ok, err := p.mem.Get(ctx, key); err != nil {
		return 0, err
	} else if ok {
		if _, perr := strconv.ParseInt(string(cur), 10, 64); perr != nil {
			return 0, core.ErrNotInteger
		}
	}
	if err := p.appendOp(encOp(op{Op: opIncr, D: delta}, key, "", nil)); err != nil {
		return 0, err
	}
	return p.mem.Incr(ctx, key, delta)
}

func (p *Provider) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	if p.closed.Load() {
		return nil, core.ErrClosed
	}
	return p.mem.MGet(ctx, keys...)
}

func (p *Provider) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	if p.closed.Load() {
		return nil, core.ErrClosed
	}
	return p.mem.Scan(ctx, start, end, limit)
}

func (p *Provider) Expire(ctx context.Context, key string, ttl int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return core.ErrClosed
	}
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	if err := p.appendOp(encOp(op{Op: opExpire, At: core.AddTTL(core.NowUnix(), ttl)}, key, "", nil)); err != nil {
		return err
	}
	return p.mem.Expire(ctx, key, ttl)
}

func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	if p.closed.Load() {
		return 0, false, core.ErrClosed
	}
	return p.mem.TTL(ctx, key)
}

// ---- Queue ----

func (p *Provider) QPush(ctx context.Context, name string, value []byte) error {
	return p.qpush(ctx, name, value, false)
}

func (p *Provider) QPushFront(ctx context.Context, name string, value []byte) error {
	return p.qpush(ctx, name, value, true)
}

func (p *Provider) qpush(ctx context.Context, name string, value []byte, front bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return core.ErrClosed
	}
	if err := p.appendOp(encOp(op{Op: opQPush, F: front}, name, "", value)); err != nil {
		return err
	}
	if front {
		return p.mem.QPushFront(ctx, name, value)
	}
	return p.mem.QPush(ctx, name, value)
}

func (p *Provider) QPop(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpop(ctx, name, false)
}

func (p *Provider) QPopBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpop(ctx, name, true)
}

func (p *Provider) qpop(ctx context.Context, name string, back bool) ([]byte, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return nil, false, core.ErrClosed
	}
	// 空队列不写日志（否则回放时多出一条无对应元素的 qpop）。
	// 已持写锁，peek 与 pop 之间无并发插入。
	var peek func(context.Context, string) ([]byte, bool, error)
	if back {
		peek = p.mem.QBack
	} else {
		peek = p.mem.QFront
	}
	if _, ok, err := peek(ctx, name); err != nil || !ok {
		return nil, false, err
	}
	if err := p.appendOp(op{Op: opQPop, K: name, F: back}); err != nil {
		return nil, false, err
	}
	if back {
		return p.mem.QPopBack(ctx, name)
	}
	return p.mem.QPop(ctx, name)
}

func (p *Provider) QSize(ctx context.Context, name string) (int64, error) {
	if p.closed.Load() {
		return 0, core.ErrClosed
	}
	return p.mem.QSize(ctx, name)
}

func (p *Provider) QFront(ctx context.Context, name string) ([]byte, bool, error) {
	if p.closed.Load() {
		return nil, false, core.ErrClosed
	}
	return p.mem.QFront(ctx, name)
}

func (p *Provider) QBack(ctx context.Context, name string) ([]byte, bool, error) {
	if p.closed.Load() {
		return nil, false, core.ErrClosed
	}
	return p.mem.QBack(ctx, name)
}

// ---- ZSet ----

func (p *Provider) ZSet(ctx context.Context, name, key string, score int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return core.ErrClosed
	}
	if err := p.appendOp(encOp(op{Op: opZSet, S: score}, name, key, nil)); err != nil {
		return err
	}
	return p.mem.ZSet(ctx, name, key, score)
}

func (p *Provider) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	if p.closed.Load() {
		return 0, false, core.ErrClosed
	}
	return p.mem.ZGet(ctx, name, key)
}

func (p *Provider) ZDel(ctx context.Context, name, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return core.ErrClosed
	}
	if err := p.appendOp(encOp(op{Op: opZDel}, name, key, nil)); err != nil {
		return err
	}
	return p.mem.ZDel(ctx, name, key)
}

func (p *Provider) ZSize(ctx context.Context, name string) (int64, error) {
	if p.closed.Load() {
		return 0, core.ErrClosed
	}
	return p.mem.ZSize(ctx, name)
}

func (p *Provider) ZRank(ctx context.Context, name, key string) (int64, bool, error) {
	if p.closed.Load() {
		return 0, false, core.ErrClosed
	}
	return p.mem.ZRank(ctx, name, key)
}

func (p *Provider) ZRange(ctx context.Context, name string, start, stop int64) ([]core.ZItem, error) {
	if p.closed.Load() {
		return nil, core.ErrClosed
	}
	return p.mem.ZRange(ctx, name, start, stop)
}

func (p *Provider) ZIncr(ctx context.Context, name, key string, delta int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return 0, core.ErrClosed
	}
	if err := p.appendOp(encOp(op{Op: opZIncr, D: delta}, name, key, nil)); err != nil {
		return 0, err
	}
	return p.mem.ZIncr(ctx, name, key, delta)
}

// Compact 将日志压缩为当前状态的等价操作序列：先写临时文件，fsync 后原子改名，
// 再重新打开新文件追加。期间处于锁内，阻塞其他操作。
func (p *Provider) Compact(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return core.ErrClosed
	}
	_ = ctx
	s := p.mem.Snapshot()
	tmp := p.path + ".compact.tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("jsonl: compact create: %w", err)
	}
	bw := bufio.NewWriterSize(f, writeBufSize)
	write := func(rec op) error {
		b, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		if len(b)+1 > MaxRecordBytes {
			return fmt.Errorf("jsonl: record too large during compact: %d bytes (limit %d)", len(b)+1, MaxRecordBytes)
		}
		_, err = bw.Write(append(b, '\n'))
		return err
	}
	for k, v := range s.KV {
		if err := write(encOp(op{Op: opSet}, k, "", v)); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	// 过期时间保留绝对戳（确定性回放）；已过期的 key 已在快照中剔除。
	for k, at := range s.Exp {
		if err := write(encOp(op{Op: opExpire, At: at}, k, "", nil)); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	for name, vals := range s.Queue {
		for _, v := range vals {
			if err := write(encOp(op{Op: opQPush}, name, "", v)); err != nil {
				f.Close()
				os.Remove(tmp)
				return err
			}
		}
	}
	for name, m := range s.ZSet {
		for k, score := range m {
			if err := write(encOp(op{Op: opZSet, S: score}, name, k, nil)); err != nil {
				f.Close()
				os.Remove(tmp)
				return err
			}
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, p.path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("jsonl: compact rename: %w", err)
	}
	syncDirBestEffort(p.path) // 让 rename 的目录项变更尽量持久（sync 模式）
	// 原子换名后重新打开追加句柄，保持与后续写路径一致。
	nf, err := os.OpenFile(p.path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		// 换名已成功而旧句柄指向的是被 unlink 的孤儿 inode：若继续服务，
		// 后续写会"返回成功却全部丢失"。这里置为已关闭，宁可拒绝服务。
		p.closed.Store(true)
		return fmt.Errorf("jsonl: compact reopen: %w", err)
	}
	p.file.Close() // 旧句柄关闭失败不影响新句柄可用性，忽略
	p.file = nf
	p.bw = bufio.NewWriterSize(nf, writeBufSize)
	return nil
}

// syncDirBestEffort 尽力 fsync 父目录，使 create/rename 的目录项变更在掉电后
// 也持久（POSIX 语义；不支持目录 fsync 的平台如部分 Windows 场景静默忽略）。
func syncDirBestEffort(path string) {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return nil
	}
	p.closed.Store(true)
	err1 := p.bw.Flush()
	err2 := p.file.Sync()
	err3 := p.file.Close()
	p.mem.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return err3
}

// enc 编码值：合法 UTF-8 存原文，否则 base64；返回编码串与是否 base64。
func enc(v []byte) (string, bool) {
	if utf8.Valid(v) {
		return string(v), false
	}
	return base64.StdEncoding.EncodeToString(v), true
}

// encStr 是 enc 的字符串版：用于 key / 队列名 / zset 成员。json.Marshal 会把
// 非法 UTF-8 字符串静默替换成 U+FFFD，导致回放重建出另一个 key（原数据
// 不可达），因此非 UTF-8 的 key/成员同样必须 base64 落盘。
func encStr(s string) (string, bool) {
	if utf8.ValidString(s) {
		return s, false
	}
	return base64.StdEncoding.EncodeToString([]byte(s)), true
}

// encOp 填充一条记录的 K/M/V 三个编码字段（写路径统一入口，回放侧 apply
// 按各字段的 b64 标志对称解码）。
func encOp(rec op, key, member string, value []byte) op {
	rec.K, rec.KB = encStr(key)
	rec.M, rec.MB = encStr(member)
	rec.V, rec.B64 = enc(value)
	return rec
}

func dec(s string, b64 bool) ([]byte, error) {
	if b64 {
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("jsonl: corrupt base64 record: %w", err)
		}
		return b, nil
	}
	return []byte(s), nil
}
