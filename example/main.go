// Command example 演示 kvdb 适配器：同一段业务代码（KV + Queue + ZSet +
// TTL + Scan + Incr）在 7 个基座间原样切换，仅改 kvdb.Open 的 URI。
//
// 用法：
//
//	go run ./example -backend mem
//	go run ./example -backend jsonl://./tmp/data.jsonl
//	go run ./example -backend sqlite://./tmp/data.db
//	go run ./example -backend mysql://root:kvdbroot@127.0.0.1:43306/kvdb_test
//	go run ./example -backend pg://postgres:kvdbpass@127.0.0.1:45432/kvdb_test?sslmode=disable
//	go run ./example -backend redis://127.0.0.1:46379/0
//	go run ./example -backend ssdb://127.0.0.1:48888
//
// mysql/pg/redis/ssdb 需先运行对应服务，连接失败会直接报错退出。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"

	"kvdb"
	_ "kvdb/all" // 示例需要全部基座可用；正式服务可按需 import 单个基座包
)

func main() {
	uri := flag.String("backend", "mem://", "存储基座 URI，见 kvdb.Open 支持的 scheme")
	flag.Parse()
	ctx := context.Background()

	// 文件型基座（jsonl/sqlite）使用示例路径时，先确保父目录存在
	//（基座本身不自动建目录）。
	if u, err := url.Parse(*uri); err == nil && (u.Scheme == "jsonl" || u.Scheme == "sqlite") {
		p := u.Path
		if u.Host != "" {
			p = u.Host + u.Path
		}
		if dir := filepath.Dir(filepath.Clean(p)); dir != "." {
			os.MkdirAll(dir, 0o755)
		}
	}

	db, err := kvdb.Open(ctx, *uri)
	if err != nil {
		log.Fatalf("打开基座 %s: %v", *uri, err)
	}
	defer db.Close()

	hasQ, hasZ, hasBatch := db.Capabilities()
	fmt.Printf("== 基座 %s（queue=%v zset=%v batch=%v） ==\n", *uri, hasQ, hasZ, hasBatch)

	// ---- KV ----
	if err := db.Set(ctx, "user:1", []byte("alice")); err != nil {
		log.Fatal(err)
	}
	db.Set(ctx, "user:2", []byte("bob"))
	db.Set(ctx, "user:3", []byte("carol"))

	if v, ok, _ := db.Get(ctx, "user:1"); ok {
		fmt.Printf("Get user:1 = %s\n", v)
	}
	if n, _ := db.Incr(ctx, "visits", 1); n > 0 {
		fmt.Printf("Incr visits = %d\n", n)
	}
	db.Expire(ctx, "visits", 3600) // 1 小时有效

	// SetEx：一次写入值与 TTL（对应 Redis SETEX / SSDB setx）
	if err := db.SetEx(ctx, "session:1", []byte("token-abc"), 1800); err != nil {
		log.Fatal(err)
	}
	if secs, ok, _ := db.TTL(ctx, "session:1"); ok {
		fmt.Printf("SetEx session:1 剩余 TTL ≈ %ds\n", secs)
	}

	fmt.Println("Scan user:1..user:3:")
	for _, kv := range must(db.Scan(ctx, "user:1", "user:3", 10)) {
		fmt.Printf("  %s = %s\n", kv.Key, kv.Value)
	}
	if secs, ok, _ := db.TTL(ctx, "visits"); ok {
		fmt.Printf("visits TTL ≈ %ds\n", secs)
	}

	// ---- Queue ----
	if hasQ {
		db.QPush(ctx, "jobs", []byte("job-1"))
		db.QPush(ctx, "jobs", []byte("job-2"))
		db.QPushFront(ctx, "jobs", []byte("job-0"))
		for n := must(db.QSize(ctx, "jobs")); n > 0; n = must(db.QSize(ctx, "jobs")) {
			v, _, _ := db.QPop(ctx, "jobs")
			fmt.Printf("QPop jobs = %s\n", v)
		}
	} else {
		fmt.Println("（该基座未实现 Queue，跳过）")
	}

	// ---- ZSet ----
	if hasZ {
		db.ZSet(ctx, "rank", "alice", 90)
		db.ZSet(ctx, "rank", "bob", 70)
		db.ZSet(ctx, "rank", "carol", 80)
		db.ZIncr(ctx, "rank", "bob", 15) // bob -> 85
		fmt.Print("ZRange rank:")
		for _, it := range must(db.ZRange(ctx, "rank", 0, -1)) {
			fmt.Printf(" %s=%d", it.Key, it.Score)
		}
		fmt.Println()
	} else {
		fmt.Println("（该基座未实现 ZSet，跳过）")
	}

	// ---- Batch（可选能力）：一批操作一次提交 ----
	if hasBatch {
		err := db.Batch(ctx, func(b *kvdb.Batch) error {
			b.Set("batch:1", []byte("v1"))
			b.QPush("jobs", []byte("job-batch"))
			b.ZIncr("rank", "alice", 5)
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
		alice, _, err := db.ZGet(ctx, "rank", "alice")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Batch 提交 3 条操作（jobs=%d, alice=%d）\n", must(db.QSize(ctx, "jobs")), alice)
	} else {
		fmt.Println("（该基座未实现 Batch，跳过）")
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}
