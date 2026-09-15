package ssdb_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/kvdbtest"
	"github.com/RelicOfTesla/kvdb/ssdb"
)

// TestRealSSDB 仅在显式设置 KVDB_TEST_SSDB_ADDR 时对真实 SSDB 服务跑行为用例：
//
//	KVDB_TEST_SSDB_ADDR=127.0.0.1:48888 go test ./ssdb/
//
// 用例前按协议直接发送 flushdb 清空库（不给生产基座加测试专用接口），
// 因此地址必须指向专用测试实例。
func TestRealSSDB(t *testing.T) {
	addr := os.Getenv("KVDB_TEST_SSDB_ADDR")
	if addr == "" {
		t.Skip("未设置 KVDB_TEST_SSDB_ADDR，跳过真实 SSDB 测试")
	}
	kvdbtest.Run(t, func(t *testing.T) core.KvProvider {
		if err := flushDB(addr); err != nil {
			t.Fatal(err)
		}
		p, err := ssdb.Open(context.Background(), addr)
		if err != nil {
			t.Fatal(err)
		}
		return p
	})
}

// flushDB 按 SSDB 块协议发送单条 flushdb：记录 "7\nflushdb\n" + 报文结束 "\n"，
// 响应按帧解析并校验状态为 ok。
func flushDB(addr string) error {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.Write([]byte("7\nflushdb\n\n")); err != nil {
		return err
	}
	br := bufio.NewReader(c)
	readRec := func() ([]byte, error) {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if line == "" {
			return nil, nil // 报文结束
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("bad size %q", line)
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(br, body); err != nil {
			return nil, err
		}
		if _, err := br.ReadString('\n'); err != nil { // 记录尾标
			return nil, err
		}
		return body, nil
	}
	status, err := readRec()
	if err != nil {
		return fmt.Errorf("flushdb 读取响应: %w", err)
	}
	if string(status) != "ok" {
		return fmt.Errorf("flushdb 返回 %q", status)
	}
	return nil
}

// TestRealSSDBAuth 对开启 server.auth 的真实 SSDB 验证认证链路：
// KVDB_TEST_SSDB_AUTH_ADDR + KVDB_TEST_SSDB_AUTH_PASS（如 127.0.0.1:48889）。
// 未认证连接命令被拒（noauth -> ErrAuth），错误密码连接失败，正确密码可读写。
func TestRealSSDBAuth(t *testing.T) {
	addr := os.Getenv("KVDB_TEST_SSDB_AUTH_ADDR")
	pass := os.Getenv("KVDB_TEST_SSDB_AUTH_PASS")
	if addr == "" || pass == "" {
		t.Skip("未设置 KVDB_TEST_SSDB_AUTH_ADDR / KVDB_TEST_SSDB_AUTH_PASS，跳过真实 SSDB 认证测试")
	}
	ctx := context.Background()

	// 未认证：命令被 noauth 拒绝
	p0, err := ssdb.Open(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := p0.Get(ctx, "k"); !errors.Is(err, ssdb.ErrAuth) {
		t.Fatalf("未认证 Get 应 ErrAuth, got %v", err)
	}
	p0.Close()

	// 错误密码：连接失败
	if _, err := ssdb.OpenWithConfig(ctx, ssdb.Config{Addr: addr, Password: "wrong-password"}); !errors.Is(err, ssdb.ErrAuth) {
		t.Fatalf("错误密码应 ErrAuth, got %v", err)
	}

	// 正确密码：可用
	p, err := ssdb.OpenWithConfig(ctx, ssdb.Config{Addr: addr, Password: pass})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("认证后 Set: %v", err)
	}
	if v, ok, _ := p.Get(ctx, "k"); !ok || string(v) != "v" {
		t.Fatalf("认证后 Get = %q,%v", v, ok)
	}

	// 显式 Auth 必须是池级的：Open 时不带密码（首连未认证），Auth 一次之后
	// 池中所有连接（含后续新建）都要能正常收发。
	p2, err := ssdb.OpenWithConfig(ctx, ssdb.Config{Addr: addr, PoolSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if err := p2.Auth(ctx, pass); err != nil {
		t.Fatalf("显式 Auth: %v", err)
	}
	const g = 32 // 远超 PoolSize，强制反复建连/复用
	var wg sync.WaitGroup
	errCh := make(chan error, g)
	for i := 0; i < g; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			k := fmt.Sprintf("authpool:%d", i)
			if err := p2.Set(ctx, k, []byte("v")); err != nil {
				errCh <- fmt.Errorf("Set(%s): %w", k, err)
				return
			}
			if v, ok, err := p2.Get(ctx, k); err != nil || !ok || string(v) != "v" {
				errCh <- fmt.Errorf("Get(%s) = %q,%v,%v", k, v, ok, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("池内存在未认证连接: %v", err)
	}
}
