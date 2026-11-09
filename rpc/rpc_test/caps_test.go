package rpc_test

import (
	"context"
	"testing"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/mem"
	"github.com/RelicOfTesla/kvdb/rpc"
)

// capsDeclaringProvider 只暴露 KvProvider 的直连方法，能力**全部**经自己的
// Capabilities() 声明——这正是代理型/包装型基座回答能力问题的唯一方式：
// 它的方法集里没有 QueueProvider / ZSetProvider / BatchProvider / IncrWrapsProvider，
// 因此任何"从类型断言重建 Caps"的实现都只能得出零值。
type capsDeclaringProvider struct {
	core.KvProvider
	caps core.Caps
}

func (p capsDeclaringProvider) Capabilities() core.Caps { return p.caps }

// TestCapabilitiesForwardsBackendDeclaration 钉住 RPC 服务端**如实透传基座声明**。
//
// 历史缺陷（与 858a09c 在 adapter.Capabilities 修过的是同一类，只是低了一层）：
// rpc.openBackend 之后的 capabilitiesOf 用类型断言从零重建 Caps，完全忽略基座
// 自己的 Capabilities()。于是任何"能力只能由声明回答"的基座（代理、包装层、
// 非 Go 直连实现）经 RPC 之后都被静默退化成默认值（Queue/ZSet/Batch/
// BatchComposed/IncrWraps 全为 false）。声明退化不会让任何一条共享用例失败
// ——它们只会按**错误的声明**分支断言，能力差异就此蒙混过关。
//
// 本基线的 core.Caps 只有 Queue/ZSet/Batch/BatchComposed/IncrWraps 五维，
// 故门禁按这五维断言；今后 Caps 增维时，本用例的 want 必须同步增维，否则
// "未覆盖的维被重建丢弃"仍会漏过。
func TestCapabilitiesForwardsBackendDeclaration(t *testing.T) {
	want := core.Caps{
		Queue:         true,
		ZSet:          true,
		Batch:         true,
		BatchComposed: true,
		IncrWraps:     true,
	}
	srv, addr := startServer(t, rpc.ServerConfig{Opener: func(context.Context) (core.KvProvider, error) {
		return capsDeclaringProvider{KvProvider: mem.New(), caps: want}, nil
	}})
	if got := srv.Capabilities(); got != want {
		t.Fatalf("服务端自身能力 = %+v，want %+v（服务端应采信基座的声明）", got, want)
	}

	p, err := rpc.Open(t.Context(), addr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if got := p.Capabilities(); got != want {
		t.Fatalf("client 能力 = %+v，want %+v（基座声明在服务端被重建/丢弃了）", got, want)
	}
}
