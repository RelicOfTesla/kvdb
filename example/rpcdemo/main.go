// Command rpcdemo 在一个进程里演示"本地变远程"：起一个 RPC 服务端（底座自选），
// 再用一个**完全不感知底座**的客户端经网络访问它。
//
// 用法：
//
//	go run ./example/rpcdemo                          # 默认 mem 底座 + 挑战认证
//	go run ./example/rpcdemo -backend jsonl://./tmp/rpc.jsonl
//	go run ./example/rpcdemo -backend sqlite://./tmp/rpc.db -auth plain
//	go run ./example/rpcdemo -tls                     # 自签证书 + TLS
//
// 真实部署时服务端与客户端是两个进程（见 ./rpcserver），这里为了便于观察
// 放在一起，并把两者之间的"隔离"直接打印出来。
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/RelicOfTesla/kvdb"
	_ "github.com/RelicOfTesla/kvdb/all" // 服务端侧要能打开所选的底座
	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/rpc"
)

func main() {
	backend := flag.String("backend", "mem://", "服务端底座 URI")
	authMode := flag.String("auth", "challenge", "c/s 认证：none | plain | challenge")
	password := flag.String("password", "s3cret", "认证口令")
	useTLS := flag.Bool("tls", false, "启用 TLS（自签证书）")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mode, err := rpc.ParseAuthMode(*authMode)
	if err != nil {
		log.Fatalf("auth 参数: %v", err)
	}
	cfg := rpc.ServerConfig{
		Network:  "tcp",
		Addr:     "127.0.0.1:0", // 让内核分配端口
		Backend:  *backend,
		Auth:     mode,
		Password: *password,
	}
	// 文件型底座先建父目录（基座本身不自动建目录）。
	// 注意相对路径（jsonl://./tmp/x）会被 url.Parse 放进 Host，需要拼回来。
	if u, err := url.Parse(*backend); err == nil {
		switch u.Scheme {
		case "jsonl", "sqlite", "bolt", "leveldb", "badger":
			p := u.Path
			if u.Host != "" {
				p = u.Host + u.Path
			}
			if dir := filepath.Dir(filepath.Clean(p)); dir != "." {
				_ = os.MkdirAll(dir, 0o755)
			}
		}
	}
	if *useTLS {
		// 服务端要证书；客户端侧通过 URI 的 tls=1&insecure=1 配置
		// （自签证书无法用系统根校验），因此这里只构造服务端配置。
		cert, _ := selfSigned()
		cfg.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	}

	srv, err := rpc.NewServer(ctx, cfg)
	if err != nil {
		log.Fatalf("起服务端: %v", err)
	}
	serveCtx, stopServe := context.WithCancel(ctx)
	defer stopServe()
	go func() { _ = srv.Serve(serveCtx) }()
	defer srv.Close()

	transport := "明文"
	if *useTLS {
		transport = "TLS"
	}
	fmt.Printf("服务端：addr=%s 底座=%s 认证=%s 传输=%s\n", srv.Addr(), *backend, mode, transport)
	fmt.Printf("服务端底座能力：%+v\n\n", srv.Capabilities())

	// ---- 以下就是"客户端"：它对底座一无所知 ----
	uri := fmt.Sprintf("rpc://%s", srv.Addr())
	q := url.Values{}
	q.Set("auth", mode.String())
	q.Set("password", *password)
	if *useTLS {
		q.Set("tls", "1")
		q.Set("insecure", "1") // 演示用自签证书；生产请用 ca= 指定信任根
	}
	uri += "?" + q.Encode()

	db, err := kvdb.Open(ctx, uri)
	if err != nil {
		log.Fatalf("客户端连接: %v", err)
	}
	defer db.Close()
	fmt.Printf("客户端 URI：%s\n", uri)
	fmt.Printf("客户端看到的能力：%+v\n\n", db.Capabilities())

	// KV + TTL
	must(db.Set(ctx, "user:1", []byte("alice")))
	must(db.SetEx(ctx, "session", []byte("tok"), 3600))
	v, ok, err := db.Get(ctx, "user:1")
	must(err)
	fmt.Printf("Get(user:1) = %q (ok=%v)\n", v, ok)
	if secs, ok, err := db.TTL(ctx, "session"); err == nil && ok {
		fmt.Printf("TTL(session) = %ds\n", secs)
	}

	// Queue
	must(db.QPush(ctx, "jobs", []byte("job-1")))
	must(db.QPush(ctx, "jobs", []byte("job-2")))
	if n, err := db.QSize(ctx, "jobs"); err == nil {
		fmt.Printf("QSize(jobs) = %d\n", n)
	}
	if v, ok, err := db.QPop(ctx, "jobs"); err == nil && ok {
		fmt.Printf("QPop(jobs) = %q\n", v)
	}

	// ZSet
	must(db.ZSet(ctx, "rank", "alice", 10))
	must(db.ZSet(ctx, "rank", "bob", 30))
	if items, err := db.ZRange(ctx, "rank", 0, -1); err == nil {
		fmt.Printf("ZRange(rank) = %v\n", items)
	}

	// 批量写（一次往返）
	must(db.Batch(ctx, func(b *kvdb.Batch) error {
		b.Set("k1", []byte("v1"))
		b.Set("k2", []byte("v2"))
		b.ZIncr("rank", "alice", 5)
		return nil
	}))
	if sc, ok, _ := db.ZGet(ctx, "rank", "alice"); ok {
		fmt.Printf("批写后 ZGet(rank,alice) = %d\n", sc)
	}

	// 扫描
	if kvs, err := db.Scan(ctx, "k", "kz", 10); err == nil {
		fmt.Printf("Scan(k..kz) = %v\n", kvs)
	}

	// 哨兵错误原样过线：错误口令 / 非法 TTL
	badQ := url.Values{}
	badQ.Set("auth", mode.String())
	badQ.Set("password", "definitely-wrong")
	if *useTLS {
		badQ.Set("tls", "1")
		badQ.Set("insecure", "1")
	}
	if _, err := kvdb.Open(ctx, "rpc://"+srv.Addr()+"?"+badQ.Encode()); err != nil {
		fmt.Printf("错误口令被拒：%v（errors.Is(ErrAuthFailed)=%v）\n",
			err, errors.Is(err, rpc.ErrAuthFailed))
	}
	if err := db.Expire(ctx, "user:1", 0); err != nil {
		fmt.Printf("Expire(ttl=0): %v\n  errors.Is(err, core.ErrInvalidTTL) = %v\n",
			err, errors.Is(err, core.ErrInvalidTTL))
	}

	// 演示"客户端 Close 不影响服务端"：关掉后再开一个客户端，数据仍在。
	must(db.Close())
	db2, err := kvdb.Open(ctx, uri)
	must(err)
	defer db2.Close()
	if v, ok, _ := db2.Get(ctx, "user:1"); ok {
		fmt.Printf("\n新客户端仍能读到 user:1 = %q（客户端 Close 未关闭服务端底座）\n", v)
	}
	fmt.Println("\n演示结束")
}

func must(err error) {
	if err != nil {
		log.Fatalf("操作失败: %v", err)
	}
}

// selfSigned 生成演示用自签证书。
func selfSigned() (tls.Certificate, *x509.CertPool) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		log.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, _ := x509.MarshalECPrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	return cert, pool
}
