package rpc_test

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
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/rpc"
)

// 本文件覆盖认证（三种模式）、TLS 与 codec 替换。
// 认证是 c/s 自己的协议，与底座自身的 auth 无关，因此这里用 mem 底座即可，
// 把注意力集中在这一层。

const testPassword = "s3cret-pw"

// TestAuthNoneDefault: 不显式开认证时，连接直接可用。
func TestAuthNoneDefault(t *testing.T) {
	_, addr := startServer(t, rpc.ServerConfig{Opener: memBackend(t)})
	p, err := rpc.OpenWithConfig(t.Context(), rpc.Config{Addr: addr})
	if err != nil {
		t.Fatalf("默认（无认证）应可连接: %v", err)
	}
	defer p.Close()
	if err := p.Set(t.Context(), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
}

// TestAuthPlain: 明文模式正确口令可连、错误口令被拒。
func TestAuthPlain(t *testing.T) {
	_, addr := startServer(t, rpc.ServerConfig{
		Opener: memBackend(t), Auth: rpc.AuthPlain, Password: testPassword,
	})
	ctx := context.Background()

	good, err := rpc.OpenWithConfig(ctx, rpc.Config{Addr: addr, Auth: rpc.AuthPlain, Password: testPassword})
	if err != nil {
		t.Fatalf("正确口令应可连接: %v", err)
	}
	defer good.Close()
	if err := good.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	_, err = rpc.OpenWithConfig(ctx, rpc.Config{Addr: addr, Auth: rpc.AuthPlain, Password: "wrong"})
	if !errors.Is(err, rpc.ErrAuthFailed) {
		t.Fatalf("错误口令应 ErrAuthFailed, got %v", err)
	}
	// 不带口令（Auth=None）连一个开了认证的服务端：必须被拒。
	_, err = rpc.OpenWithConfig(ctx, rpc.Config{Addr: addr})
	if err == nil {
		t.Fatal("未认证的连接不应被接受")
	}
}

// TestAuthChallenge: 挑战模式正确/错误口令，并验证服务端确实没收到口令本身。
func TestAuthChallenge(t *testing.T) {
	_, addr := startServer(t, rpc.ServerConfig{
		Opener: memBackend(t), Auth: rpc.AuthChallenge, Password: testPassword,
	})
	ctx := context.Background()

	good, err := rpc.OpenWithConfig(ctx, rpc.Config{Addr: addr, Auth: rpc.AuthChallenge, Password: testPassword})
	if err != nil {
		t.Fatalf("正确口令应可连接: %v", err)
	}
	defer good.Close()
	if err := good.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := good.Get(ctx, "k"); !ok || string(v) != "v" {
		t.Fatalf("Get = %q,%v", v, ok)
	}

	_, err = rpc.OpenWithConfig(ctx, rpc.Config{Addr: addr, Auth: rpc.AuthChallenge, Password: "wrong"})
	if !errors.Is(err, rpc.ErrAuthFailed) {
		t.Fatalf("错误口令应 ErrAuthFailed, got %v", err)
	}
}

// TestAuthChallengeDoesNotLeakPassword 是本文件最重要的一条：
// 抓取挑战握手的原始字节，断言口令**从未出现在线路上**。
// 这正是挑战模式相对明文模式的意义所在。
func TestAuthChallengeDoesNotLeakPassword(t *testing.T) {
	const pw = "leak-canary-9f3a"
	// 起一个会记录原始流量的 server。
	raw := &recordingListener{}
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	raw.Listener = base

	srv, err := rpc.NewServer(context.Background(), rpc.ServerConfig{
		Opener: memBackend(t), Auth: rpc.AuthChallenge, Password: pw,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.ServeWithListener(raw) }()
	defer srv.Close()

	p, err := rpc.OpenWithConfig(context.Background(), rpc.Config{
		Addr: base.Addr().String(), Auth: rpc.AuthChallenge, Password: pw,
	})
	if err != nil {
		t.Fatalf("challenge 认证应成功: %v", err)
	}
	defer p.Close()
	if err := p.Set(context.Background(), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	wire := raw.snapshot()
	if len(wire) == 0 {
		t.Fatal("未捕获到任何流量")
	}
	if strings.Contains(string(wire), pw) {
		t.Fatal("挑战模式的线路上不应出现口令明文")
	}
	// 反向确认录制确实有效：命令内容应该在流量里。
	if !strings.Contains(string(wire), "AUTHCHAL") {
		t.Fatal("录制流量里应能看到 AUTHCHAL 握手")
	}
}

// TestAuthPlainLeaksByDesign 对照说明明文模式的取舍：
// 它**确实**把口令写在线上——这正是它默认不打开的原因。
func TestAuthPlainLeaksByDesign(t *testing.T) {
	const pw = "plain-canary-4b7c"
	raw := &recordingListener{}
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	raw.Listener = base

	srv, err := rpc.NewServer(context.Background(), rpc.ServerConfig{
		Opener: memBackend(t), Auth: rpc.AuthPlain, Password: pw,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.ServeWithListener(raw) }()
	defer srv.Close()

	p, err := rpc.OpenWithConfig(context.Background(), rpc.Config{
		Addr: base.Addr().String(), Auth: rpc.AuthPlain, Password: pw,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if !strings.Contains(string(raw.snapshot()), pw) {
		t.Fatal("明文模式应当能在线路上看到口令（这也是它默认关闭的原因）")
	}
}

// TestDataCommandsRequireAuth: 未认证就发数据命令必须被拒（auth_required）。
func TestDataCommandsRequireAuth(t *testing.T) {
	_, addr := startServer(t, rpc.ServerConfig{
		Opener: memBackend(t), Auth: rpc.AuthChallenge, Password: testPassword,
	})
	// 绕过 client 的握手逻辑，直接裸连发一条 SET。
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if _, err := nc.Write([]byte("$3\r\nSET\r\n*2\r\n$1\r\nk\r\n$1\r\nv\r\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, err := nc.Read(buf)
	if err != nil {
		t.Fatalf("读取应答: %v", err)
	}
	got := string(buf[:n])
	if !strings.Contains(got, "auth_required") {
		t.Fatalf("未认证的数据命令应被拒, got %q", got)
	}
}

// TestTLSRoundTrip: 标准 crypto/tls，同一端口按配置切换。
func TestTLSRoundTrip(t *testing.T) {
	cert, caPEM := selfSignedCert(t, "127.0.0.1")

	_, addr := startServer(t, rpc.ServerConfig{
		Opener:    memBackend(t),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	})
	ctx := context.Background()

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("无法加载测试 CA")
	}
	p, err := rpc.OpenWithConfig(ctx, rpc.Config{
		Addr: addr,
		TLSConfig: &tls.Config{
			RootCAs:    pool,
			ServerName: "127.0.0.1",
			MinVersion: tls.VersionTLS12,
		},
	})
	if err != nil {
		t.Fatalf("TLS 连接应成功: %v", err)
	}
	defer p.Close()
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := p.Get(ctx, "k"); !ok || string(v) != "v" {
		t.Fatalf("TLS 下 Get = %q,%v", v, ok)
	}

	// 不用 TLS 去连 TLS 端口：必须失败，而不是静默降级成明文。
	plain, err := rpc.OpenWithConfig(ctx, rpc.Config{Addr: addr})
	if err == nil {
		plain.Close()
		t.Fatal("对 TLS 端口用明文连接不应成功（不得静默降级）")
	}
}

// TestTLSWithAuth: TLS 与挑战认证叠加使用（推荐的生产组合）。
func TestTLSWithAuth(t *testing.T) {
	cert, caPEM := selfSignedCert(t, "127.0.0.1")
	_, addr := startServer(t, rpc.ServerConfig{
		Opener:    memBackend(t),
		Auth:      rpc.AuthChallenge,
		Password:  testPassword,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	})
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	p, err := rpc.OpenWithConfig(context.Background(), rpc.Config{
		Addr:      addr,
		Auth:      rpc.AuthChallenge,
		Password:  testPassword,
		TLSConfig: &tls.Config{RootCAs: pool, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12},
	})
	if err != nil {
		t.Fatalf("TLS+认证应成功: %v", err)
	}
	defer p.Close()
	if err := p.Set(context.Background(), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
}

// TestTLSConfigErrors: 配置错误要显式报错，不能悄悄退化成明文。
func TestTLSConfigErrors(t *testing.T) {
	for _, uri := range []string{
		"rpc://127.0.0.1:1?ca=./nope.pem",         // 给了 TLS 选项但没开 tls=1
		"rpc://127.0.0.1:1?tls=1&ca=./nope.pem",   // CA 文件不存在
		"rpc://127.0.0.1:1?tls=1&cert=./c.pem",    // 只给 cert 没给 key
		"rpc://127.0.0.1:1?auth=bogus&password=x", // 未知认证模式
		"rpc://127.0.0.1:1?password=x",            // 给了口令却没写 auth=
		"rpc://127.0.0.1:1?auth=plain&password=",  // 选了明文却没口令
		"rpc://127.0.0.1:1?codec=nope",            // 未知 codec
		"rpc://127.0.0.1:1?pool=0",                // 非法池大小
	} {
		if _, err := rpc.OpenURI(context.Background(), mustURL(t, uri)); err == nil {
			t.Fatalf("URI %q 应报错", uri)
		}
	}
}

// TestCodecBinary: 换 codec 后全套功能仍然工作（证明抽象是真的）。
func TestCodecBinary(t *testing.T) {
	_, addr := startServer(t, rpc.ServerConfig{Opener: memBackend(t), Codec: rpc.CodecBinary})
	ctx := context.Background()

	p, err := rpc.OpenWithConfig(ctx, rpc.Config{Addr: addr, Codec: rpc.CodecBinary})
	if err != nil {
		t.Fatalf("binary codec 连接: %v", err)
	}
	defer p.Close()

	// 含空参数、空值、二进制值、以及需要保序的多值返回。
	if err := p.Set(ctx, "empty", []byte{}); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := p.Get(ctx, "empty"); !ok || len(v) != 0 {
		t.Fatalf("空 value: %q,%v", v, ok)
	}
	bin := []byte{0x00, 0x01, 0xff, '\r', '\n', '$', '*'}
	if err := p.Set(ctx, "bin", bin); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := p.Get(ctx, "bin"); !ok || string(v) != string(bin) {
		t.Fatalf("二进制 value 往返不一致: %v", v)
	}
	if _, err := p.Scan(ctx, "", "", 10); err != nil {
		t.Fatalf("空 start/end 的 Scan: %v", err)
	}
	for _, m := range []string{"a", "b", "c"} {
		if err := p.ZSet(ctx, "z", m, 1); err != nil {
			t.Fatal(err)
		}
	}
	items, err := p.ZRange(ctx, "z", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 || items[0].Key != "a" || items[2].Key != "c" {
		t.Fatalf("ZRange 应保序: %+v", items)
	}
}

// TestCodecMismatchFails: 两端 codec 不一致必须**明确失败**，
// 而不是解析出乱七八糟的数据继续跑。
func TestCodecMismatchFails(t *testing.T) {
	_, addr := startServer(t, rpc.ServerConfig{Opener: memBackend(t)}) // 默认 RESP
	ctx := context.Background()
	p, err := rpc.OpenWithConfig(ctx, rpc.Config{Addr: addr, Codec: rpc.CodecBinary})
	if err == nil {
		// 若握手侥幸通过，后续命令也必须失败。
		if err2 := p.Set(ctx, "k", []byte("v")); err2 == nil {
			p.Close()
			t.Fatal("codec 不匹配不应正常工作")
		}
		p.Close()
		return
	}
	if !errors.Is(err, rpc.ErrProtocol) && !errors.Is(err, rpc.ErrAuthFailed) {
		t.Logf("codec 不匹配的错误（可接受）: %v", err)
	}
}

// TestRESPHandWrittenRequest 验证"能用 nc/redis-cli 手测"这条调试路径：
// 手工拼出的 RESP 请求应被服务端正确接受。
func TestRESPHandWrittenRequest(t *testing.T) {
	_, addr := startServer(t, rpc.ServerConfig{Opener: memBackend(t)})
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	// SET k v
	if _, err := nc.Write([]byte("$3\r\nSET\r\n*2\r\n$1\r\nk\r\n$1\r\nv\r\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 128)
	_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, err := nc.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); !strings.HasPrefix(got, ":1\r\n+ok\r\n") {
		t.Fatalf("手写 SET 的应答 = %q", got)
	}
	// GET k -> 单块 v
	if _, err := nc.Write([]byte("$3\r\nGET\r\n*1\r\n$1\r\nk\r\n")); err != nil {
		t.Fatal(err)
	}
	n, err = nc.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	got := string(buf[:n])
	if !strings.Contains(got, "+ok") || !strings.Contains(got, "1\r\nv\r\n") {
		t.Fatalf("手写 GET 的应答 = %q", got)
	}
}

// TestBatchTooLargeRejected: 超限批写被拒，避免一个坏请求打爆服务端内存。
func TestBatchTooLargeRejected(t *testing.T) {
	_, addr := startServer(t, rpc.ServerConfig{Opener: memBackend(t), MaxBatchOps: 4})
	p, err := rpc.Open(t.Context(), addr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ops := make([]core.BatchOp, 0, 10)
	for i := 0; i < 10; i++ {
		ops = append(ops, core.BatchOp{Kind: core.BatchSet, Key: "k", Value: []byte("v")})
	}
	if err := p.ApplyBatch(context.Background(), ops); err == nil {
		t.Fatal("超过 MaxBatchOps 的批写应被拒绝")
	}
}

// ---- 测试辅助 ----

// recordingListener 记录 Accept 出来的连接上读到的原始字节，
// 用于断言线路上到底发了什么。
type recordingListener struct {
	net.Listener
	mu   sync.Mutex
	data []byte
}

func (l *recordingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &recordingConn{Conn: c, l: l}, nil
}

func (l *recordingListener) snapshot() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]byte(nil), l.data...)
}

func (l *recordingListener) record(p []byte) {
	l.mu.Lock()
	l.data = append(l.data, p...)
	l.mu.Unlock()
}

type recordingConn struct {
	net.Conn
	l *recordingListener
}

func (c *recordingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.l.record(p[:n])
	}
	return n, err
}

// selfSignedCert 生成一张自签证书，返回证书与 PEM 编码的 CA（即该证书本身）。
func selfSignedCert(t *testing.T, host string) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: host},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	} else {
		tmpl.DNSNames = []string{host}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return cert, certPEM
}
