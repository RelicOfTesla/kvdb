// Command cli 是一个类似 redis-cli 的命令行工具：连到任意 kvdb 基座
// （本地嵌入式或 rpc:// 远程），手打命令做查看/调试。
//
// 用法：
//
//	# 一次性执行（适合脚本）
//	go run ./example/cmd/cli -backend sqlite://./tmp/a.db SET k v
//	go run ./example/cmd/cli -backend sqlite://./tmp/a.db GET k
//
//	# 交互模式（不带命令即进入）
//	go run ./example/cmd/cli -backend mem://
//	kvdb> SET k v
//	OK
//	kvdb> GET k
//	"v"
//
//	# 连远程 RPC 服务端（见 example/rpcdemo）
//	go run ./example/cmd/cli -backend 'rpc://127.0.0.1:7788?auth=challenge&password=s3cret'
//
// 命令名与 RPC 报文里的方法名一致（SET/GET/QPUSH/…），因此同一套用法对本地基座
// 与远程服务端都成立。**命令本身不区分大小写**。
//
// 输入解析沿用 shell 风格：参数按空白切分，支持单/双引号包裹、以及 \" \\ \n 等
// 反斜杠转义，便于传入含空格的 key 或二进制脏值（如 $'\x00\xff' 形态不受支持，
// 请用 \\xNN 转义）。
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/RelicOfTesla/kvdb"

	// 与 example 其它命令一致：一次接入全部内置基座，因此 -backend 可任意指定。
	_ "github.com/RelicOfTesla/kvdb/all"
)

func main() {
	backend := flag.String("backend", "mem://", "基座 URI（本地/文件型/rpc:// 均可）")
	timeout := flag.Duration("timeout", 10*time.Second, "单条命令超时")
	quiet := flag.Bool("q", false, "一次性模式下只打印结果，不打印提示")
	flag.Usage = usage
	flag.Parse()

	ctx := context.Background()
	db, err := kvdb.Open(ctx, *backend)
	if err != nil {
		fmt.Fprintf(os.Stderr, "打开基座 %s: %v\n", *backend, err)
		os.Exit(1)
	}
	defer db.Close()

	c := &cli{db: db, timeout: *timeout, out: os.Stdout}

	// 一次性模式：把剩余参数当一条命令执行。
	if args := flag.Args(); len(args) > 0 {
		code := c.exec(ctx, args)
		os.Exit(code)
	}
	_ = quiet
	os.Exit(c.repl(ctx, *backend))
}

func usage() {
	fmt.Fprintf(os.Stderr, `kvdb-cli —— 连到任意 kvdb 基座并手打命令

用法:
  cli [-backend URI] [-timeout 10s] [COMMAND [ARG...]]

不带 COMMAND 时进入交互模式。示例:
  cli -backend mem:// SET k v
  cli -backend 'rpc://127.0.0.1:7788' GET k

常用命令:
  SET key value | SETEX key ttl value | SETEXAT key at value | GET key | DEL key
  EXISTS key | INCR key [delta] | MGET key... | SCAN [start] [end] [limit]
  EXPIRE key ttl | EXPIREAT key at | TTL key
  QPUSH name value | QPUSHFRONT name value | QPOP name | QPOPBACK name
  QSIZE name | QFRONT name | QBACK name | QRANGE name [start] [stop]
  ZSET name member score | ZGET name member | ZDEL name member | ZINCR name member [delta]
  ZSIZE name | ZRANK name member | ZRANGE name start stop
  CAPS | PING | HELP | QUIT
`)
}

type cli struct {
	db      kvdb.DB
	timeout time.Duration
	out     *os.File
}

// repl 从 stdin 逐行读命令，直到 EOF/QUIT/EXIT。
func (c *cli) repl(ctx context.Context, backend string) int {
	fmt.Fprintf(c.out, "kvdb-cli 已连接 %s（HELP 看命令，QUIT 退出）\n", backend)
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024) // 允许较长的 value
	for {
		fmt.Fprint(c.out, "kvdb> ")
		if !sc.Scan() {
			// Scan 返回 false 有两种原因：EOF（正常退出）或读错误。
			// 必须区分——把读错误静默当成 EOF，会让脚本误以为输入已正常读完。
			if err := sc.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "读取输入: %v\n", err)
				return 1
			}
			fmt.Fprintln(c.out)
			return 0
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		args, err := splitArgs(line)
		if err != nil {
			fmt.Fprintf(c.out, "(解析错误) %v\n", err)
			continue
		}
		if len(args) == 0 {
			continue
		}
		switch strings.ToUpper(args[0]) {
		case "QUIT", "EXIT":
			return 0
		}
		c.exec(ctx, args)
	}
}

// exec 执行一条命令并打印结果；返回进程退出码（仅供一次性模式使用）。
func (c *cli) exec(ctx context.Context, args []string) int {
	cmd := strings.ToUpper(args[0])
	rest := args[1:]

	// HELP/PING 不碰基座，先处理。
	switch cmd {
	case "HELP":
		usage()
		return 0
	case "PING":
		// 用一个最小读写探测连通性（各基座都支持 Set/Get）。
		if err := c.db.Set(ctx, "__kvdb_cli_ping__", []byte("1")); err != nil {
			c.errf("%v", err)
			return 1
		}
		_ = c.db.Del(ctx, "__kvdb_cli_ping__")
		fmt.Fprintln(c.out, "PONG")
		return 0
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.dispatch(ctx, cmd, rest)
}

// dispatch 按命令分派。每个分支只做"取参 → 调 SDK → 打印"，保持与 SDK 一一对应，
// 不在 CLI 里发明语义（例如空队列就是 (nil) 而不是错误）。
func (c *cli) dispatch(ctx context.Context, cmd string, a []string) int {
	need := func(n int, form string) bool {
		if len(a) < n {
			c.errf("用法: %s", form)
			return false
		}
		return true
	}
	switch cmd {
	// ---- KV ----
	case "SET":
		if !need(2, "SET key value") {
			return 1
		}
		if err := c.db.Set(ctx, a[0], []byte(a[1])); err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, "OK")
	case "SETEX":
		if !need(3, "SETEX key ttl value") {
			return 1
		}
		ttl, err := c.int(a[1], "ttl")
		if err != nil {
			return 1
		}
		if err := c.db.SetEx(ctx, a[0], []byte(a[2]), ttl); err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, "OK")
	case "SETEXAT":
		if !need(3, "SETEXAT key at value") {
			return 1
		}
		at, err := c.int(a[1], "at")
		if err != nil {
			return 1
		}
		if err := c.db.SetExAt(ctx, a[0], []byte(a[2]), at); err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, "OK")
	case "GET":
		if !need(1, "GET key") {
			return 1
		}
		v, ok, err := c.db.Get(ctx, a[0])
		if err != nil {
			return c.errf("%v", err)
		}
		if !ok {
			fmt.Fprintln(c.out, "(nil)")
			return 0
		}
		fmt.Fprintln(c.out, quote(v))
	case "DEL":
		if !need(1, "DEL key") {
			return 1
		}
		if err := c.db.Del(ctx, a[0]); err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, "OK")
	case "EXISTS":
		if !need(1, "EXISTS key") {
			return 1
		}
		ok, err := c.db.Exists(ctx, a[0])
		if err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, boolInt(ok))
	case "INCR":
		if !need(1, "INCR key [delta]") {
			return 1
		}
		var delta int64 = 1
		if len(a) >= 2 {
			d, err := c.int(a[1], "delta")
			if err != nil {
				return 1
			}
			delta = d
		}
		n, err := c.db.Incr(ctx, a[0], delta)
		if err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, n)
	case "MGET":
		if !need(1, "MGET key [key...]") {
			return 1
		}
		m, err := c.db.MGet(ctx, a...)
		if err != nil {
			return c.errf("%v", err)
		}
		for _, k := range a {
			if v, ok := m[k]; ok {
				fmt.Fprintf(c.out, "%s = %s\n", k, quote(v))
			} else {
				fmt.Fprintf(c.out, "%s = (nil)\n", k)
			}
		}
	case "SCAN":
		start, end := "", ""
		limit := 0
		if len(a) >= 1 {
			start = a[0]
		}
		if len(a) >= 2 {
			end = a[1]
		}
		if len(a) >= 3 {
			n, err := c.int(a[2], "limit")
			if err != nil {
				return 1
			}
			limit = int(n)
		}
		kvs, err := c.db.Scan(ctx, start, end, limit)
		if err != nil {
			return c.errf("%v", err)
		}
		if len(kvs) == 0 {
			fmt.Fprintln(c.out, "(empty)")
		}
		for _, kv := range kvs {
			fmt.Fprintf(c.out, "%s = %s\n", kv.Key, quote(kv.Value))
		}
	case "EXPIRE":
		if !need(2, "EXPIRE key ttl") {
			return 1
		}
		ttl, err := c.int(a[1], "ttl")
		if err != nil {
			return 1
		}
		if err := c.db.Expire(ctx, a[0], ttl); err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, "OK")
	case "EXPIREAT":
		if !need(2, "EXPIREAT key at") {
			return 1
		}
		at, err := c.int(a[1], "at")
		if err != nil {
			return 1
		}
		if err := c.db.ExpireAt(ctx, a[0], at); err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, "OK")
	case "TTL":
		if !need(1, "TTL key") {
			return 1
		}
		secs, ok, err := c.db.TTL(ctx, a[0])
		if err != nil {
			return c.errf("%v", err)
		}
		if !ok {
			fmt.Fprintln(c.out, "(no ttl)")
			return 0
		}
		fmt.Fprintln(c.out, secs)

	// ---- Queue ----
	case "QPUSH", "QPUSHFRONT":
		if !need(2, cmd+" name value") {
			return 1
		}
		var err error
		if cmd == "QPUSH" {
			err = c.db.QPush(ctx, a[0], []byte(a[1]))
		} else {
			err = c.db.QPushFront(ctx, a[0], []byte(a[1]))
		}
		if err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, "OK")
	case "QPOP", "QPOPBACK", "QFRONT", "QBACK":
		if !need(1, cmd+" name") {
			return 1
		}
		var (
			v   []byte
			ok  bool
			err error
		)
		switch cmd {
		case "QPOP":
			v, ok, err = c.db.QPop(ctx, a[0])
		case "QPOPBACK":
			v, ok, err = c.db.QPopBack(ctx, a[0])
		case "QFRONT":
			v, ok, err = c.db.QFront(ctx, a[0])
		default:
			v, ok, err = c.db.QBack(ctx, a[0])
		}
		if err != nil {
			return c.errf("%v", err)
		}
		if !ok {
			fmt.Fprintln(c.out, "(nil)")
			return 0
		}
		fmt.Fprintln(c.out, quote(v))
	case "QRANGE":
		if !need(1, "QRANGE name [start] [stop]") {
			return 1
		}
		start, stop := int64(0), int64(-1)
		if len(a) >= 2 {
			s, err := c.int(a[1], "start")
			if err != nil {
				return 1
			}
			start = s
		}
		if len(a) >= 3 {
			s, err := c.int(a[2], "stop")
			if err != nil {
				return 1
			}
			stop = s
		}
		items, err := c.db.QRange(ctx, a[0], start, stop)
		if err != nil {
			return c.errf("%v", err)
		}
		if len(items) == 0 {
			fmt.Fprintln(c.out, "(empty)")
		}
		for i, v := range items {
			fmt.Fprintf(c.out, "%d) %s\n", i, quote(v))
		}
	case "QSIZE":
		if !need(1, "QSIZE name") {
			return 1
		}
		n, err := c.db.QSize(ctx, a[0])
		if err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, n)

	// ---- ZSet ----
	case "ZSET":
		if !need(3, "ZSET name member score") {
			return 1
		}
		score, err := c.int(a[2], "score")
		if err != nil {
			return 1
		}
		if err := c.db.ZSet(ctx, a[0], a[1], score); err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, "OK")
	case "ZGET":
		if !need(2, "ZGET name member") {
			return 1
		}
		s, ok, err := c.db.ZGet(ctx, a[0], a[1])
		if err != nil {
			return c.errf("%v", err)
		}
		if !ok {
			fmt.Fprintln(c.out, "(nil)")
			return 0
		}
		fmt.Fprintln(c.out, s)
	case "ZDEL":
		if !need(2, "ZDEL name member") {
			return 1
		}
		if err := c.db.ZDel(ctx, a[0], a[1]); err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, "OK")
	case "ZINCR":
		if !need(2, "ZINCR name member [delta]") {
			return 1
		}
		var delta int64 = 1
		if len(a) >= 3 {
			d, err := c.int(a[2], "delta")
			if err != nil {
				return 1
			}
			delta = d
		}
		s, err := c.db.ZIncr(ctx, a[0], a[1], delta)
		if err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, s)
	case "ZSIZE":
		if !need(1, "ZSIZE name") {
			return 1
		}
		n, err := c.db.ZSize(ctx, a[0])
		if err != nil {
			return c.errf("%v", err)
		}
		fmt.Fprintln(c.out, n)
	case "ZRANK":
		if !need(2, "ZRANK name member") {
			return 1
		}
		r, ok, err := c.db.ZRank(ctx, a[0], a[1])
		if err != nil {
			return c.errf("%v", err)
		}
		if !ok {
			fmt.Fprintln(c.out, "(nil)")
			return 0
		}
		fmt.Fprintln(c.out, r)
	case "ZRANGE":
		start, stop := int64(0), int64(-1)
		if len(a) >= 2 {
			s, err := c.int(a[1], "start")
			if err != nil {
				return 1
			}
			start = s
		}
		if len(a) >= 3 {
			s, err := c.int(a[2], "stop")
			if err != nil {
				return 1
			}
			stop = s
		}
		if !need(1, "ZRANGE name [start] [stop]") {
			return 1
		}
		items, err := c.db.ZRange(ctx, a[0], start, stop)
		if err != nil {
			return c.errf("%v", err)
		}
		if len(items) == 0 {
			fmt.Fprintln(c.out, "(empty)")
		}
		for i, it := range items {
			fmt.Fprintf(c.out, "%d) %s = %d\n", i, it.Key, it.Score)
		}

	// ---- 能力 ----
	case "CAPS":
		caps := c.db.Capabilities()
		fmt.Fprintf(c.out, "queue=%v zset=%v batch=%v batch_composed=%v\n",
			caps.Queue, caps.ZSet, caps.Batch, caps.BatchComposed)

	default:
		c.errf("未知命令 %q（HELP 看命令列表）", cmd)
		return 1
	}
	return 0
}

func (c *cli) int(s, name string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		c.errf("%s 不是整数: %q", name, s)
		return 0, err
	}
	return n, nil
}

func (c *cli) errf(format string, a ...any) int {
	fmt.Fprintf(c.out, "(错误) "+format+"\n", a...)
	return 1
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// quote 尽量可读地打印值：可打印 ASCII 直接引号包裹，否则给十六进制。
// 与 redis-cli 的做法一致——二进制值不硬塞进终端。
func quote(v []byte) string {
	if len(v) == 0 {
		return `""`
	}
	printable := true
	for _, b := range v {
		if b < 0x20 || b > 0x7e {
			printable = false
			break
		}
	}
	if printable {
		return fmt.Sprintf("%q", string(v))
	}
	return fmt.Sprintf("%x", v)
}
