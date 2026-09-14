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
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"kvdb"
	"kvdb/core"
	"kvdb/mem"
)

func init() { kvdb.MustRegister("jsonl", OpenURI) }

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

// op 是日志的一行记录；字段为各数据结构所需的最小集合。
type op struct {
	Op  string `json:"op"`            // set|setx|del|incr|expire|qpush|qpop|zset|zdel|zincr
	K   string `json:"k,omitempty"`   // kv key / 队列名 / zset 名
	M   string `json:"m,omitempty"`   // zset 成员
	V   string `json:"v,omitempty"`   // set / qpush 的值（文本或 base64）
	B64 bool   `json:"b64,omitempty"` // V 为 base64 编码
	D   int64  `json:"d,omitempty"`   // incr / zincr 增量
	S   int64  `json:"s,omitempty"`   // zset 分数
	At  int64  `json:"at,omitempty"`  // expire 绝对过期时间（unix 秒，必然 >0）
	F   bool   `json:"f,omitempty"`   // qpush 到队头 / qpop 从队尾
}

// Provider 是 JSONL 基座。并发安全；ctx 仅用于接口一致。
type Provider struct {
	mu     sync.Mutex
	mem    *mem.Provider
	file   *os.File
	bw     *bufio.Writer
	path   string
	sync   bool
	closed bool
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
		bw:   bufio.NewWriterSize(f, 32*1024),
		path: path,
		sync: cfg.Sync,
	}
	if err := p.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return p, nil
}

// replay 打开时回放日志。遇首个无法解析的行即停止：尾部半行是崩溃残留，
// 属正常容错；若坏行之后还有完整行则说明日志损坏，返回错误。
func (p *Provider) replay() error {
	if _, err := p.file.Seek(0, 0); err != nil {
		return fmt.Errorf("jsonl: seek: %w", err)
	}
	sc := bufio.NewScanner(p.file)
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024) // 单值上限 64MiB
	var lines []string
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("jsonl: read: %w", err)
	}
	now := time.Now().Unix()
	for i, line := range lines {
		if line == "" {
			continue
		}
		var rec op
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			if i == len(lines)-1 {
				break // 容忍崩溃残留的尾部半行
			}
			return fmt.Errorf("jsonl: replay line %d: %w", i+1, err)
		}
		if err := p.apply(rec, now); err != nil {
			return fmt.Errorf("jsonl: replay line %d: %w", i+1, err)
		}
	}
	return nil
}

// apply 将一条记录作用到内存状态（与写路径顺序一致即可确定性还原）。
func (p *Provider) apply(rec op, now int64) error {
	switch rec.Op {
	case "set":
		return p.mem.Set(context.Background(), rec.K, dec(rec.V, rec.B64))
	case "setx":
		if err := p.mem.Set(context.Background(), rec.K, dec(rec.V, rec.B64)); err != nil {
			return err
		}
		remain := rec.At - now
		if remain <= 0 {
			return p.mem.Del(context.Background(), rec.K)
		}
		return p.mem.Expire(context.Background(), rec.K, remain)
	case "del":
		return p.mem.Del(context.Background(), rec.K)
	case "incr":
		_, err := p.mem.Incr(context.Background(), rec.K, rec.D)
		return err
	case "expire":
		remain := rec.At - now
		if remain <= 0 {
			return p.mem.Del(context.Background(), rec.K)
		}
		return p.mem.Expire(context.Background(), rec.K, remain)
	case "qpush":
		if rec.F {
			return p.mem.QPushFront(context.Background(), rec.K, dec(rec.V, rec.B64))
		}
		return p.mem.QPush(context.Background(), rec.K, dec(rec.V, rec.B64))
	case "qpop":
		if rec.F {
			_, _, err := p.mem.QPopBack(context.Background(), rec.K)
			return err
		}
		_, _, err := p.mem.QPop(context.Background(), rec.K)
		return err
	case "zset":
		return p.mem.ZSet(context.Background(), rec.K, rec.M, rec.S)
	case "zdel":
		return p.mem.ZDel(context.Background(), rec.K, rec.M)
	case "zincr":
		_, err := p.mem.ZIncr(context.Background(), rec.K, rec.M, rec.D)
		return err
	default:
		return fmt.Errorf("unknown op %q", rec.Op)
	}
}

// appendOp 追加一条记录并落缓冲（Sync 模式下立即 fsync）。
func (p *Provider) appendOp(rec op) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("jsonl: marshal: %w", err)
	}
	if _, err := p.bw.Write(append(b, '\n')); err != nil {
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

// ---- KV ----

func (p *Provider) Set(ctx context.Context, key string, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return core.ErrClosed
	}
	if err := p.mem.Set(ctx, key, value); err != nil {
		return err
	}
	s, b64 := enc(value)
	return p.appendOp(op{Op: "set", K: key, V: s, B64: b64})
}

// SetEx 写入 value 并覆盖 TTL（对应 Redis SETEX / SSDB setx）；
// 日志记录绝对过期时间，回放可确定性还原。
func (p *Provider) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return core.ErrClosed
	}
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	if err := p.mem.SetEx(ctx, key, value, ttl); err != nil {
		return err
	}
	s, b64 := enc(value)
	return p.appendOp(op{Op: "setx", K: key, V: s, B64: b64, At: time.Now().Unix() + ttl})
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, false, core.ErrClosed
	}
	return p.mem.Get(ctx, key)
}

func (p *Provider) Del(ctx context.Context, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return core.ErrClosed
	}
	if err := p.mem.Del(ctx, key); err != nil {
		return err
	}
	return p.appendOp(op{Op: "del", K: key})
}

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false, core.ErrClosed
	}
	return p.mem.Exists(ctx, key)
}

func (p *Provider) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, core.ErrClosed
	}
	n, err := p.mem.Incr(ctx, key, delta)
	if err != nil {
		return 0, err
	}
	return n, p.appendOp(op{Op: "incr", K: key, D: delta})
}

func (p *Provider) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, core.ErrClosed
	}
	return p.mem.MGet(ctx, keys...)
}

func (p *Provider) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, core.ErrClosed
	}
	return p.mem.Scan(ctx, start, end, limit)
}

func (p *Provider) Expire(ctx context.Context, key string, ttl int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return core.ErrClosed
	}
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	if err := p.mem.Expire(ctx, key, ttl); err != nil {
		return err
	}
	return p.appendOp(op{Op: "expire", K: key, At: time.Now().Unix() + ttl})
}

func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
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
	if p.closed {
		return core.ErrClosed
	}
	if front {
		if err := p.mem.QPushFront(ctx, name, value); err != nil {
			return err
		}
	} else if err := p.mem.QPush(ctx, name, value); err != nil {
		return err
	}
	s, b64 := enc(value)
	return p.appendOp(op{Op: "qpush", K: name, V: s, B64: b64, F: front})
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
	if p.closed {
		return nil, false, core.ErrClosed
	}
	var v []byte
	var ok bool
	var err error
	if back {
		v, ok, err = p.mem.QPopBack(ctx, name)
	} else {
		v, ok, err = p.mem.QPop(ctx, name)
	}
	if err != nil || !ok {
		return v, ok, err
	}
	return v, ok, p.appendOp(op{Op: "qpop", K: name, F: back})
}

func (p *Provider) QSize(ctx context.Context, name string) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, core.ErrClosed
	}
	return p.mem.QSize(ctx, name)
}

func (p *Provider) QFront(ctx context.Context, name string) ([]byte, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, false, core.ErrClosed
	}
	return p.mem.QFront(ctx, name)
}

func (p *Provider) QBack(ctx context.Context, name string) ([]byte, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, false, core.ErrClosed
	}
	return p.mem.QBack(ctx, name)
}

// ---- ZSet ----

func (p *Provider) ZSet(ctx context.Context, name, key string, score int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return core.ErrClosed
	}
	if err := p.mem.ZSet(ctx, name, key, score); err != nil {
		return err
	}
	return p.appendOp(op{Op: "zset", K: name, M: key, S: score})
}

func (p *Provider) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, false, core.ErrClosed
	}
	return p.mem.ZGet(ctx, name, key)
}

func (p *Provider) ZDel(ctx context.Context, name, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return core.ErrClosed
	}
	if err := p.mem.ZDel(ctx, name, key); err != nil {
		return err
	}
	return p.appendOp(op{Op: "zdel", K: name, M: key})
}

func (p *Provider) ZSize(ctx context.Context, name string) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, core.ErrClosed
	}
	return p.mem.ZSize(ctx, name)
}

func (p *Provider) ZRank(ctx context.Context, name, key string) (int64, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, false, core.ErrClosed
	}
	return p.mem.ZRank(ctx, name, key)
}

func (p *Provider) ZRange(ctx context.Context, name string, start, stop int64) ([]core.ZItem, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, core.ErrClosed
	}
	return p.mem.ZRange(ctx, name, start, stop)
}

func (p *Provider) ZIncr(ctx context.Context, name, key string, delta int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, core.ErrClosed
	}
	n, err := p.mem.ZIncr(ctx, name, key, delta)
	if err != nil {
		return 0, err
	}
	return n, p.appendOp(op{Op: "zincr", K: name, M: key, D: delta})
}

// Compact 将日志压缩为当前状态的等价操作序列：先写临时文件，fsync 后原子改名，
// 再重新打开新文件追加。期间处于锁内，阻塞其他操作。
func (p *Provider) Compact(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return core.ErrClosed
	}
	_ = ctx
	s := p.mem.Snapshot()
	tmp := p.path + ".compact.tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("jsonl: compact create: %w", err)
	}
	bw := bufio.NewWriterSize(f, 32*1024)
	write := func(rec op) error {
		b, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		_, err = bw.Write(append(b, '\n'))
		return err
	}
	for k, v := range s.KV {
		vs, b64 := enc(v)
		if err := write(op{Op: "set", K: k, V: vs, B64: b64}); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	// 过期时间保留绝对戳（确定性回放）；已过期的 key 已在快照中剔除。
	for k, at := range s.Exp {
		if err := write(op{Op: "expire", K: k, At: at}); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	for name, vals := range s.Queue {
		for _, v := range vals {
			vs, b64 := enc(v)
			if err := write(op{Op: "qpush", K: name, V: vs, B64: b64}); err != nil {
				f.Close()
				os.Remove(tmp)
				return err
			}
		}
	}
	for name, m := range s.ZSet {
		for k, score := range m {
			if err := write(op{Op: "zset", K: name, M: k, S: score}); err != nil {
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
	// 原子换名后重新打开追加句柄，保持与后续写路径一致。
	nf, err := os.OpenFile(p.path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("jsonl: compact reopen: %w", err)
	}
	p.file.Close()
	p.file = nf
	p.bw = bufio.NewWriterSize(nf, 32*1024)
	return nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
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

func dec(s string, b64 bool) []byte {
	if b64 {
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil // 记录损坏时按空值回放（容错），不阻断启动
		}
		return b
	}
	return []byte(s)
}
