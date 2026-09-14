package ssdb_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/internal/behaviortest"
	"github.com/RelicOfTesla/kvdb/ssdb"
)

// fakeSSDB 是进程内 SSDB 服务器替身：按 SSDB wiki/源码（link.cpp）定义的
// 原生文本协议独立实现收发，用于交叉验证客户端编码与命令语义。
type fakeSSDB struct {
	mu        sync.Mutex
	kv        map[string][]byte
	exp       map[string]int64 // key -> 绝对过期秒
	queue     map[string][][]byte
	zset      map[string]map[string]int64
	ln        net.Listener
	password  string       // 非空则要求先 auth（对应 server.auth）
	dropAfter atomic.Int32 // >0 时每个连接处理完 N 条请求后主动断开（模拟服务器断连）
}

func newFakeSSDB(t *testing.T) *fakeSSDB {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeSSDB{
		kv:    map[string][]byte{},
		exp:   map[string]int64{},
		queue: map[string][][]byte{},
		zset:  map[string]map[string]int64{},
		ln:    ln,
	}
	go f.serve()
	t.Cleanup(func() { ln.Close() })
	return f
}

func (f *fakeSSDB) addr() string { return f.ln.Addr().String() }

func (f *fakeSSDB) now() int64 { return time.Now().Unix() }

func (f *fakeSSDB) alive(key string) ([]byte, bool) {
	if e, ok := f.exp[key]; ok && e <= f.now() {
		delete(f.kv, key)
		delete(f.exp, key)
		return nil, false
	}
	v, ok := f.kv[key]
	return v, ok
}

func (f *fakeSSDB) serve() {
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(c)
	}
}

// 记录读写（与客户端同样的 <size>\n<body>\n ... 空行结尾 帧格式）。
type recordReader struct{ br *bufio.Reader }

func (r *recordReader) read() ([][]byte, error) {
	var recs [][]byte
	for {
		line, err := r.br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if line == "" {
			return recs, nil
		}
		n, err := strconv.Atoi(line)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("bad size %q", line)
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r.br, body); err != nil {
			return nil, err
		}
		if _, err := r.br.ReadString('\n'); err != nil { // 记录尾标
			return nil, err
		}
		recs = append(recs, body)
	}
}

func writeRecs(bw *bufio.Writer, recs ...[]byte) error {
	for _, r := range recs {
		if _, err := bw.WriteString(strconv.Itoa(len(r)) + "\n"); err != nil {
			return err
		}
		if _, err := bw.Write(r); err != nil {
			return err
		}
		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
	}
	return bw.WriteByte('\n')
}

func (f *fakeSSDB) handle(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	bw := bufio.NewWriter(c)
	rr := &recordReader{br}
	authed := false
	served := 0
	for {
		// 模拟服务器在处理 N 条请求后、读取下一条之前主动断开
		//（如重启/超时）。放在 read 之前，保证断开时客户端尚未发出
		// 下一条请求——死连接才会真的留在客户端池中等待探测。
		if da := int(f.dropAfter.Load()); da > 0 && served >= da {
			return
		}
		req, err := rr.read()
		if err != nil {
			return
		}
		if len(req) == 0 {
			continue
		}
		served++
		var reply [][]byte
		cmd := string(req[0])
		switch {
		case cmd == "auth":
			// 与 SSDB proc_auth 一致：密码正确（或服务端未设密码）回 ok/1，
			// 否则 error/invalid password。
			if f.password == "" || (len(req) == 2 && string(req[1]) == f.password) {
				authed = true
				reply = [][]byte{[]byte("ok"), []byte("1")}
			} else {
				reply = [][]byte{[]byte("error"), []byte("invalid password")}
			}
		case f.password != "" && !authed:
			// 未认证时其余命令被拒（server.cpp 的 AUTH 前置检查）。
			reply = [][]byte{[]byte("noauth"), []byte("authentication required.")}
		default:
			reply = f.dispatch(req)
		}
		if err := writeRecs(bw, reply...); err != nil {
			return
		}
		if err := bw.Flush(); err != nil {
			return
		}
	}
}

func s(b []byte) string { return string(b) }

func (f *fakeSSDB) dispatch(req [][]byte) [][]byte {
	cmd := s(req[0])
	arg := func(i int) string { return s(req[i]) }
	ok1 := func() [][]byte { return [][]byte{[]byte("ok"), []byte("1")} }

	switch cmd {
	case "set":
		f.mu.Lock()
		f.kv[arg(1)] = append([]byte(nil), req[2]...)
		f.mu.Unlock()
		return ok1()
	case "setx":
		// setx key value ttl：写值并设 TTL（与 proc_setx 一致）。
		f.mu.Lock()
		ttl, _ := strconv.ParseInt(arg(3), 10, 64)
		f.kv[arg(1)] = append([]byte(nil), req[2]...)
		f.exp[arg(1)] = f.now() + ttl
		f.mu.Unlock()
		return ok1()
	case "get":
		f.mu.Lock()
		v, ok := f.alive(arg(1))
		f.mu.Unlock()
		if !ok {
			return [][]byte{[]byte("not_found")}
		}
		return [][]byte{[]byte("ok"), v}
	case "del":
		f.mu.Lock()
		delete(f.kv, arg(1))
		delete(f.exp, arg(1))
		f.mu.Unlock()
		return ok1()
	case "exists":
		f.mu.Lock()
		_, ok := f.alive(arg(1))
		f.mu.Unlock()
		v := "0"
		if ok {
			v = "1"
		}
		return [][]byte{[]byte("ok"), []byte(v)}
	case "incr":
		f.mu.Lock()
		defer f.mu.Unlock()
		delta, _ := strconv.ParseInt(arg(2), 10, 64)
		var cur int64
		if v, ok := f.alive(arg(1)); ok {
			n, err := strconv.ParseInt(s(v), 10, 64)
			if err != nil {
				return [][]byte{[]byte("error"), []byte("value is not an integer or out of range")}
			}
			cur = n
		}
		cur += delta
		f.kv[arg(1)] = []byte(strconv.FormatInt(cur, 10))
		return [][]byte{[]byte("ok"), []byte(strconv.FormatInt(cur, 10))}
	case "multi_get":
		f.mu.Lock()
		out := [][]byte{[]byte("ok")}
		for _, k := range req[1:] {
			if v, ok := f.alive(s(k)); ok {
				out = append(out, k, v)
			}
		}
		f.mu.Unlock()
		return out
	case "scan":
		f.mu.Lock()
		start, end := arg(1), arg(2)
		limit, _ := strconv.Atoi(arg(3))
		var keys []string
		for k := range f.kv {
			// 与真实 SSDB 一致：start 开区间、end 闭区间。
			if k <= start {
				continue
			}
			if end != "" && k > end {
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := [][]byte{[]byte("ok")}
		for i, k := range keys {
			if i >= limit {
				break
			}
			if v, ok := f.alive(k); ok {
				out = append(out, []byte(k), v)
			}
		}
		f.mu.Unlock()
		return out
	case "expire":
		f.mu.Lock()
		if _, ok := f.alive(arg(1)); ok {
			ttl, _ := strconv.ParseInt(arg(2), 10, 64)
			f.exp[arg(1)] = f.now() + ttl
			f.mu.Unlock()
			return ok1()
		}
		f.mu.Unlock()
		return [][]byte{[]byte("ok"), []byte("0")}
	case "ttl":
		f.mu.Lock()
		ex, ok := f.exp[arg(1)]
		f.mu.Unlock()
		if !ok {
			return [][]byte{[]byte("ok"), []byte("-1")}
		}
		return [][]byte{[]byte("ok"), []byte(strconv.FormatInt((ex-f.now())/1, 10))}

	case "qpush", "qpush_front":
		f.mu.Lock()
		q := f.queue[arg(1)]
		if !strings.HasSuffix(cmd, "front") {
			q = append(q, append([]byte(nil), req[2]...))
		} else {
			q = append([][]byte{append([]byte(nil), req[2]...)}, q...)
		}
		f.queue[arg(1)] = q
		f.mu.Unlock()
		return ok1()
	case "qpop", "qpop_back":
		f.mu.Lock()
		q := f.queue[arg(1)]
		var v []byte
		if len(q) == 0 {
			f.mu.Unlock()
			return [][]byte{[]byte("not_found")}
		}
		if strings.HasSuffix(cmd, "back") {
			v = q[len(q)-1]
			q = q[:len(q)-1]
		} else {
			v = q[0]
			q = q[1:]
		}
		f.queue[arg(1)] = q
		f.mu.Unlock()
		return [][]byte{[]byte("ok"), v}
	case "qsize":
		f.mu.Lock()
		n := len(f.queue[arg(1)])
		f.mu.Unlock()
		return [][]byte{[]byte("ok"), []byte(strconv.Itoa(n))}
	case "qfront", "qback":
		f.mu.Lock()
		q := f.queue[arg(1)]
		var v []byte
		if len(q) == 0 {
			f.mu.Unlock()
			return [][]byte{[]byte("not_found")}
		}
		if strings.HasSuffix(cmd, "back") {
			v = q[len(q)-1]
		} else {
			v = q[0]
		}
		f.mu.Unlock()
		return [][]byte{[]byte("ok"), v}

	case "zset":
		f.mu.Lock()
		score, _ := strconv.ParseInt(arg(3), 10, 64)
		if f.zset[arg(1)] == nil {
			f.zset[arg(1)] = map[string]int64{}
		}
		f.zset[arg(1)][arg(2)] = score
		f.mu.Unlock()
		return ok1()
	case "zget":
		f.mu.Lock()
		score, ok := f.zset[arg(1)][arg(2)]
		f.mu.Unlock()
		if !ok {
			return [][]byte{[]byte("not_found")}
		}
		return [][]byte{[]byte("ok"), []byte(strconv.FormatInt(score, 10))}
	case "zdel":
		f.mu.Lock()
		if m := f.zset[arg(1)]; m != nil {
			delete(m, arg(2))
		}
		f.mu.Unlock()
		return ok1()
	case "zincr":
		f.mu.Lock()
		delta, _ := strconv.ParseInt(arg(3), 10, 64)
		if f.zset[arg(1)] == nil {
			f.zset[arg(1)] = map[string]int64{}
		}
		f.zset[arg(1)][arg(2)] += delta
		score := f.zset[arg(1)][arg(2)]
		f.mu.Unlock()
		return [][]byte{[]byte("ok"), []byte(strconv.FormatInt(score, 10))}
	case "zrank":
		f.mu.Lock()
		m := f.zset[arg(1)]
		score, ok := m[arg(2)]
		if !ok {
			f.mu.Unlock()
			return [][]byte{[]byte("not_found")}
		}
		rank := 0
		for k, sc := range m {
			if sc < score || (sc == score && k < arg(2)) {
				rank++
			}
		}
		f.mu.Unlock()
		return [][]byte{[]byte("ok"), []byte(strconv.Itoa(rank))}
	case "zrange":
		f.mu.Lock()
		offset, _ := strconv.ParseUint(arg(2), 10, 64)
		limit, _ := strconv.ParseUint(arg(3), 10, 64)
		m := f.zset[arg(1)]
		type kv struct {
			k  string
			sc int64
		}
		var items []kv
		for k, sc := range m {
			items = append(items, kv{k, sc})
		}
		sort.Slice(items, func(i, j int) bool {
			if items[i].sc != items[j].sc {
				return items[i].sc < items[j].sc
			}
			return items[i].k < items[j].k
		})
		out := [][]byte{[]byte("ok")}
		for i := offset; i < offset+limit && i < uint64(len(items)); i++ {
			out = append(out, []byte(items[i].k), []byte(strconv.FormatInt(items[i].sc, 10)))
		}
		f.mu.Unlock()
		return out
	case "zsize":
		f.mu.Lock()
		n := len(f.zset[arg(1)])
		f.mu.Unlock()
		return [][]byte{[]byte("ok"), []byte(strconv.Itoa(n))}
	default:
		return [][]byte{[]byte("error"), []byte("unknown command: " + cmd)}
	}
}

func TestBehavior(t *testing.T) {
	srv := newFakeSSDB(t)
	behaviortest.Run(t, func(t *testing.T) core.KvProvider {
		p, err := ssdb.Open(context.Background(), srv.addr())
		if err != nil {
			t.Fatal(err)
		}
		return p
	})
}

// TestWireBinary 覆盖含换行/空格/非 UTF-8 的键值经多记录帧完整往返。
func TestWireBinary(t *testing.T) {
	srv := newFakeSSDB(t)
	p, err := ssdb.Open(context.Background(), srv.addr())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	key := "k with space\nand newline"
	val := []byte{0x00, 0xff, '\n', '\r', 0x80, ' ', 'x'}
	if err := p.Set(ctx, key, val); err != nil {
		t.Fatal(err)
	}
	got, ok, err := p.Get(ctx, key)
	if err != nil || !ok || string(got) != string(val) {
		t.Fatalf("binary roundtrip = %q,%v,%v", got, ok, err)
	}

	// scan 区间与顺序：键按字节序
	for _, k := range []string{"a1", "a2", "a3"} {
		if err := p.Set(ctx, k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	items, err := p.Scan(ctx, "a1", "a2", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Key != "a1" || items[1].Key != "a2" {
		t.Fatalf("scan = %v", items)
	}
}

// TestServerErrors 验证服务器错误状态映射。
func TestServerErrors(t *testing.T) {
	srv := newFakeSSDB(t)
	p, err := ssdb.Open(context.Background(), srv.addr())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	if err := p.Set(ctx, "bad", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Incr(ctx, "bad", 1); !errors.Is(err, core.ErrNotInteger) {
		t.Fatalf("incr 非整数应映射 ErrNotInteger, got %v", err)
	}
	if _, _, err := p.Get(ctx, "missing"); err != nil {
		t.Fatalf("get missing: %v", err)
	}
}

// newFakeSSDBWithAuth 启动要求密码的假服务器（对应 server.auth 配置）。
func newFakeSSDBWithAuth(t *testing.T, password string) *fakeSSDB {
	t.Helper()
	f := newFakeSSDB(t)
	f.password = password
	return f
}

// TestAuth 覆盖 SSDB 认证：自动认证、显式 Auth、密码错误、未认证被拒(noauth)。
func TestAuth(t *testing.T) {
	ctx := context.Background()
	const pass = "0123456789abcdef0123456789abcdef" // SSDB 要求 >=32 位强密码

	t.Run("ConfigPassword", func(t *testing.T) {
		srv := newFakeSSDBWithAuth(t, pass)
		p, err := ssdb.OpenWithConfig(ctx, ssdb.Config{Addr: srv.addr(), Password: pass})
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if err := p.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatalf("已认证连接 Set: %v", err)
		}
		if v, ok, _ := p.Get(ctx, "k"); !ok || string(v) != "v" {
			t.Fatalf("Get = %q,%v", v, ok)
		}
	})

	t.Run("ExplicitAuth", func(t *testing.T) {
		srv := newFakeSSDBWithAuth(t, pass)
		p, err := ssdb.Open(ctx, srv.addr())
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		// 未认证：读被拒并映射为 ErrAuth
		if _, _, err := p.Get(ctx, "k"); !errors.Is(err, ssdb.ErrAuth) {
			t.Fatalf("未认证命令应 ErrAuth, got %v", err)
		}
		if err := p.Auth(ctx, pass); err != nil {
			t.Fatalf("Auth: %v", err)
		}
		if err := p.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatalf("认证后 Set: %v", err)
		}
	})

	t.Run("WrongPassword", func(t *testing.T) {
		srv := newFakeSSDBWithAuth(t, pass)
		_, err := ssdb.OpenWithConfig(ctx, ssdb.Config{Addr: srv.addr(), Password: "wrong-password"})
		if !errors.Is(err, ssdb.ErrAuth) {
			t.Fatalf("错误密码应 ErrAuth, got %v", err)
		}
		p, err := ssdb.Open(ctx, srv.addr())
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if err := p.Auth(ctx, "wrong-password"); !errors.Is(err, ssdb.ErrAuth) {
			t.Fatalf("显式 Auth 错误密码应 ErrAuth, got %v", err)
		}
	})

	t.Run("URIWithPassword", func(t *testing.T) {
		srv := newFakeSSDBWithAuth(t, pass)
		u, err := url.Parse("ssdb://:" + pass + "@" + srv.addr())
		if err != nil {
			t.Fatal(err)
		}
		kp, err := ssdb.OpenURI(ctx, u)
		if err != nil {
			t.Fatal(err)
		}
		defer kp.(core.Closer).Close()
		if err := kp.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatalf("URI 密码认证后 Set: %v", err)
		}
	})

	t.Run("NoAuthServer", func(t *testing.T) {
		// 服务端未设 auth：任意密码也返回 ok（SSDB need_auth=false 分支）
		srv := newFakeSSDB(t)
		p, err := ssdb.OpenWithConfig(ctx, ssdb.Config{Addr: srv.addr(), Password: "anything"})
		if err != nil {
			t.Fatalf("无认证服务端不应报错: %v", err)
		}
		defer p.Close()
	})
}

// TestPoolConcurrency 验证连接池下并发读写的正确性与互不串扰：
// 多 goroutine 各自读写私有 key，最终值必须与各自写入一致
// （单连接时代的串行化不会暴露连接间状态残留，池化后必须验证）。
func TestPoolConcurrency(t *testing.T) {
	srv := newFakeSSDB(t)
	p, err := ssdb.OpenWithConfig(context.Background(), ssdb.Config{
		Addr:     srv.addr(),
		PoolSize: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	const g, per = 6, 50
	var wg sync.WaitGroup
	errs := make(chan error, g)
	for i := 0; i < g; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", id)
			val := []byte(fmt.Sprintf("v%d", id))
			for j := 0; j < per; j++ {
				if err := p.Set(ctx, key, val); err != nil {
					errs <- err
					return
				}
				got, ok, err := p.Get(ctx, key)
				if err != nil {
					errs <- err
					return
				}
				if !ok || string(got) != string(val) {
					errs <- fmt.Errorf("goroutine %d: Get = %q,%v want %q", id, got, ok, val)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestPoolCloseDuringOps 验证关闭后不再接受新操作且不 panic。
func TestPoolCloseDuringOps(t *testing.T) {
	srv := newFakeSSDB(t)
	p, err := ssdb.OpenWithConfig(context.Background(), ssdb.Config{Addr: srv.addr(), PoolSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Set(ctx, "k", []byte("v")); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("关闭后应 ErrClosed, got %v", err)
	}
	if err := p.Close(); err != nil { // 幂等
		t.Fatalf("重复 Close 应无错, got %v", err)
	}
}

// TestPoolReconnect 验证连接池的断连自愈：服务器每处理若干个请求就断开连接，
// 池应丢弃已断开连接并新建（业务侧操作不受影响或最多瞬断一次）。
func TestPoolReconnect(t *testing.T) {
	srv := newFakeSSDB(t)
	srv.dropAfter.Store(2) // 每个连接只服务 2 个请求后断开（模拟频繁重启）
	p, err := ssdb.OpenWithConfig(context.Background(), ssdb.Config{Addr: srv.addr(), PoolSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	// 前两个请求在同一连接上完成，服务器随后断开该连接。
	if err := p.Set(ctx, "k", []byte("v1")); err != nil {
		t.Fatalf("set1: %v", err)
	}
	if err := p.Set(ctx, "k", []byte("v2")); err != nil {
		t.Fatalf("set2: %v", err)
	}

	// 之后的请求必须自愈：连接断开时"刚好被复用到、FIN 尚未到达本地"的
	// 请求会失败一次，随后连接被丢弃重建。核心断言是失败次数有界（若死连接
	// 滞留池中则每次都会失败）且最终操作成功。
	failures := 0
	for i := 0; i < 6; i++ {
		if err := p.Set(ctx, "k", []byte("v3")); err != nil {
			failures++
		}
	}
	if failures > 3 {
		t.Fatalf("断连后 6 次操作失败 %d 次（无自愈时应为 6/6），有界失败才符合预期", failures)
	}
	// 最终读取：极端时序下最后一次请求也可能命中刚断开的连接，重试一次即可。
	var v []byte
	var ok bool
	for attempt := 0; attempt < 3; attempt++ {
		v, ok, _ = p.Get(ctx, "k")
		if ok && string(v) == "v3" {
			return
		}
	}
	t.Fatalf("最终 Get = %q,%v", v, ok)
}

// TestPoolReconnectDeterministic 确定性验证断连自愈：服务器每处理 1 条请求后
// 关闭连接并等待客户端观察到期（无竞态），后续请求必须全部成功。
func TestPoolReconnectDeterministic(t *testing.T) {
	srv := newFakeSSDB(t)
	srv.dropAfter.Store(1) // 每连接只服务 1 条请求即断开
	p, err := ssdb.OpenWithConfig(context.Background(), ssdb.Config{Addr: srv.addr(), PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()

	// 第一条在预建连接上：成功（随后服务器断开该连接）。
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("set1: %v", err)
	}
	// 等服务端 CLOSE 传播到本地（FIN 到达后连接才算真的断开）。
	time.Sleep(300 * time.Millisecond)

	// 之后每条请求：池中连接已死 -> 探测丢弃 -> 新建。全部应成功。
	//
	// 注意：这里在每次操作前等待 FIN 到达本地。服务器配置为"每服务 1 条即断开"，
	// 若紧接着复用（不等 FIN），请求会撞上"连接已断但本地尚未感知"的半开窗口——
	// 那属于已被文档接受的边界（最多消耗一次失败请求，随后仍自愈），
	// 由 TestPoolReconnect 覆盖。本用例严格断言探测路径：连接断开可观测时，
	// 池必定丢弃并重建，业务零失败。
	for i := 0; i < 5; i++ {
		time.Sleep(50 * time.Millisecond) // 上一轮连接的 FIN 到达本地
		if err := p.Set(ctx, "k", []byte("v2")); err != nil {
			t.Fatalf("断连后第 %d 条 Set 失败: %v", i, err)
		}
		time.Sleep(50 * time.Millisecond) // 服务器服务完 1 条后断开该连接
		if v, ok, _ := p.Get(ctx, "k"); !ok || string(v) != "v2" {
			t.Fatalf("第 %d 条 Get = %q,%v", i, v, ok)
		}
	}
}
