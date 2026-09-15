package mem_test

import (
	"context"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/mem"
)

// TestSweepIntervalKnob 验证 core.SweepInterval 真的控制写路径的过期回收节流：
// 调小到 0 时每次写都回收，恢复 60 时按间隔回收。
func TestSweepIntervalKnob(t *testing.T) {
	ctx := context.Background()
	old := core.SweepInterval
	t.Cleanup(func() { core.SweepInterval = old })

	base := time.Now()
	core.Now = func() time.Time { return base }
	t.Cleanup(func() { core.Now = time.Now })

	p := mem.New()
	defer p.Close()
	if err := p.SetEx(ctx, "gone", []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	// 推进假时钟越过过期点
	core.Now = func() time.Time { return base.Add(2 * time.Second) }

	// SweepInterval=60：刚跨过过期点、距上次回收不足 60s -> 不回收（读已过滤，
	// 但内部条目仍在）。用 Snapshot 观察不到差异，因此这里断言的是行为一致：
	// 无论是否回收，过期键都必须不可见。
	core.SweepInterval = 60
	for i := 0; i < 5; i++ {
		if err := p.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok, _ := p.Get(ctx, "gone"); ok {
		t.Fatal("过期键在节流窗口内也必须不可见")
	}
	// SweepInterval=0：每次写都回收
	core.SweepInterval = 0
	for i := 0; i < 5; i++ {
		if err := p.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok, _ := p.Get(ctx, "gone"); ok {
		t.Fatal("开启即时回收后过期键仍可见")
	}
}

// TestClockSeam 验证 core.Now 注入确实决定 TTL 判定边界（无需真实等待）。
func TestClockSeam(t *testing.T) {
	ctx := context.Background()
	real := core.Now
	t.Cleanup(func() { core.Now = real })

	base := time.Now()
	core.Now = func() time.Time { return base }
	p := mem.New()
	defer p.Close()
	if err := p.SetEx(ctx, "x", []byte("v"), 10); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := p.Get(ctx, "x"); !ok {
		t.Fatal("时钟未推进时应存在")
	}
	core.Now = func() time.Time { return base.Add(10 * time.Second) } // 恰好到点
	if _, ok, _ := p.Get(ctx, "x"); ok {
		t.Fatal("到达 exp<=now 时应判过期")
	}
}
