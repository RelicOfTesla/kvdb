package kvdb

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"sync"

	"kvdb/core"
)

// OpenFunc 按解析后的 URI 构造一个基座。自定义基座通过 Register 接入后，
// 即可与内置基座一样用 kvdb.Open 按 scheme 选择。
type OpenFunc func(ctx context.Context, u *url.URL) (core.KvProvider, error)

// registry 默认**为空**：根包不绑定任何基座实现，因而不引入任何驱动依赖。
// 各基座包在 init 中自行注册（显式注册模式），接入方式二选一：
//
//	import _ "kvdb/sqlite"              // 只接入需要的基座（推荐：依赖最小）
//	import _ "kvdb/all"                 // 一次性接入全部内置基座
//
// 同一 kvdb.Open(ctx, "sqlite://./db") 的调用方代码在两种方式下完全一致。
var (
	registryMu sync.RWMutex
	registry   = map[string]OpenFunc{}
)

// Register 注册 scheme 对应的基座构造函数。基座包通常在 init 中通过 MustRegister
// 调用；业务方也可直接注册自定义基座。scheme 为空或重复注册返回错误。
func Register(scheme string, open OpenFunc) error {
	if scheme == "" {
		return fmt.Errorf("kvdb: scheme must not be empty")
	}
	if open == nil {
		return fmt.Errorf("kvdb: opener for scheme %q must not be nil", scheme)
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registry[scheme]; dup {
		return fmt.Errorf("kvdb: scheme %q already registered", scheme)
	}
	registry[scheme] = open
	return nil
}

// MustRegister 是 Register 的 panic 版本，供基座包的 init 使用：
// 重复注册属于编码/装配错误，启动期即失败优于运行期静默。
func MustRegister(scheme string, open OpenFunc) {
	if err := Register(scheme, open); err != nil {
		panic(err)
	}
}

// Schemes 返回当前已注册的 scheme（字典序），便于诊断"忘了 import _ 基座包"
// 这类装配问题。
func Schemes() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]string, 0, len(registry))
	for s := range registry {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// open 是 Open 的内部实现（Open 在 provider.go 再导出一层便于文档聚合）。
func open(ctx context.Context, uri string) (*DB, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("kvdb: parse uri %q: %w", uri, err)
	}
	registryMu.RLock()
	opener, ok := registry[u.Scheme]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("kvdb: unknown provider scheme %q (registered: %v); "+
			"import the backend package (e.g. _ \"kvdb/%s\") or _ \"kvdb/all\"",
			u.Scheme, Schemes(), u.Scheme)
	}
	p, err := opener(ctx, u)
	if err != nil {
		return nil, err
	}
	return Wrap(p), nil
}

// 内置基座的 URI 约定（各 scheme 由对应基座包注册，见各包 OpenURI）：
//
//	import _ "kvdb/mem"      // 或 import _ "kvdb/all" 一次性全部接入
//
//	kvdb.Open(ctx, "mem://")
//	kvdb.Open(ctx, "jsonl://./data.jsonl?sync=1")
//	kvdb.Open(ctx, "sqlite://./data.db")
//	kvdb.Open(ctx, "mysql://user:pass@host:3306/dbname")
//	kvdb.Open(ctx, "pg://user:pass@host:5432/dbname")
//	kvdb.Open(ctx, "redis://:pass@host:6379/0")
//	kvdb.Open(ctx, "ssdb://host:8888")
