package kvdb_test

import (
	"context"
	"net/url"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

// 本文件为根模块的注册表用例提供一个"已注册的后端"，使根模块自身保持**零依赖**
// （不 import 任何真实基座模块）。它复用 mock_test.go 里的 KV / Queue 假实现，
// 通过 kvdb.Register 接入，因此走的是与真实基座完全相同的注册→Open 路径。
//
// 真实基座的"自我注册"由各基座模块自己的测试覆盖（如 mem/registry_test.go）。

// stubScheme 是测试桩注册的 scheme。刻意不用 "mem"：根模块并不 import mem，
// 用真实基座名会掩盖"根模块是否误引入后端依赖"这一问题。
const stubScheme = "stub"

// stubProvider 组合 KV + Queue 两种能力，满足用到的 db.Set/Get/Incr/QPop 等。
type stubProvider struct {
	*fakeKV
	*fakeQueue
}

func newStubProvider() *stubProvider {
	return &stubProvider{fakeKV: newFakeKV(), fakeQueue: &fakeQueue{}}
}

func init() {
	kvdb.MustRegister(stubScheme, func(context.Context, *url.URL) (core.KvProvider, error) {
		return newStubProvider(), nil
	})
}

// ApplyBatch 让测试桩具备批写能力，供"TypedDB 透传 Batch"这类用例使用：
// 支持 KV 与 Queue 的批操作，zset 相关操作显式报错（桩不实现 zset）。
func (s *stubProvider) ApplyBatch(ctx context.Context, ops []core.BatchOp) error {
	for _, op := range ops {
		var err error
		switch op.Kind {
		case core.BatchSet:
			err = s.fakeKV.Set(ctx, op.Key, op.Value)
		case core.BatchSetEx:
			err = s.fakeKV.SetEx(ctx, op.Key, op.Value, op.TTL)
		case core.BatchDel:
			err = s.fakeKV.Del(ctx, op.Key)
		case core.BatchExpire:
			err = s.fakeKV.Expire(ctx, op.Key, op.TTL)
		case core.BatchQPush:
			err = s.fakeQueue.QPush(ctx, op.Key, op.Value)
		case core.BatchQPushFront:
			err = s.fakeQueue.QPushFront(ctx, op.Key, op.Value)
		default:
			return core.ErrUnsupported // 桩不实现 zset 批操作
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// 编译期确认测试桩满足注册所需的接口。
var _ core.KvProvider = (*stubProvider)(nil)
var _ core.QueueProvider = (*stubProvider)(nil)
var _ core.BatchProvider = (*stubProvider)(nil)
