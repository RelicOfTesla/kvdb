package mem_test

import (
	"context"
	"testing"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/internal/behaviortest"
	"github.com/RelicOfTesla/kvdb/mem"
)

func TestBehavior(t *testing.T) {
	behaviortest.Run(t, func(t *testing.T) core.KvProvider {
		return mem.New()
	})
}

func TestClosed(t *testing.T) {
	p := mem.New()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.Get(context.Background(), "a"); err != core.ErrClosed {
		t.Fatalf("Close 后 Get 应 ErrClosed, got %v", err)
	}
}
