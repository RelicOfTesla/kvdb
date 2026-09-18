package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb/core"
)

// 本文件是 sqlstore（sqlite/mysql/pg/mssql 四个基座共用的实现）上
// "check()/Close() 竞态不得泄漏底层驱动错误"的回归测试。
//
// 放在 sqlstore 内部而非某个包装包：sqlstore 的 DDL/语句全部走 database/sql，
// 用一个**纯标准库**的最小驱动即可确定性复现竞态，不需要引入任何真实数据库
// 驱动，也不必起外部服务；而放在这里能一次覆盖全部四个基座。
//
// 缺陷回顾：Close() 与在途操作并发时，操作在通过 p.check()（closed 仍为 false）
// 之后、真正执行语句之前，连接池可能已经被 Close() 关闭，驱动随之返回
// "sql: database is closed"。若不把这个错误映射成 core.ErrClosed，调用方就会
// 看到一个非哨兵的驱动原始错误，破坏"关闭后一律 ErrClosed"的契约。
// 修复方式是 closedErr()：仅当 closed 已置位时才改写错误。若某个方法忘了经
// closedErr 归一（Set/Get 之外的历史实现就是这样），本测试会把它抓出来。

// driverClosedErr 与 database/sql 在连接池关闭后返回的错误一致。
// 这是**非哨兵**错误的典型代表：调用方只靠 errors.Is(err, core.ErrClosed)
// 无法识别它，因此绝不允许泄漏。
var driverClosedErr = errors.New("sql: database is closed")

// raceConnector/raceConn 是最小 database/sql 驱动，用来确定性地把测试卡在
// "已通过 check()、尚未执行完语句"的窗口上：
//   - 未被 arm 时，所有语句立即成功（供 New 的建表与过期清理通过）；
//   - arm 后，下一条进入驱动的语句先 close(entered) 通知测试、再阻塞在
//     release 上，直到测试调用 Close() 之后被放行，并返回 driverClosedErr。
//
// 这样复现的正是生产环境里的真实交错：方法先通过 check()，随后 Close() 落地，
// 方法拿到的就是驱动层的 "database is closed"。
type raceConnector struct {
	conn *raceConn
}

func (c *raceConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c *raceConnector) Driver() driver.Driver                        { return raceDriver{} }

type raceDriver struct{}

func (raceDriver) Open(string) (driver.Conn, error) { return nil, errors.New("raceDriver: use OpenDB") }

type raceConn struct {
	armed   bool
	entered chan struct{}
	release chan struct{}
}

// arm 让下一条进入驱动的语句进入阻塞态，返回 (entered, release)。
func (c *raceConn) arm() (<-chan struct{}, chan struct{}) {
	c.armed = true
	c.entered = make(chan struct{})
	c.release = make(chan struct{})
	return c.entered, c.release
}

// block 在语句进入驱动时被调用：未被 arm 返回 false（语句照常成功）；
// 被 arm 则通知测试并阻塞到 release 关闭，返回 true（语句返回关闭错误）。
func (c *raceConn) block() bool {
	if !c.armed {
		return false
	}
	c.armed = false
	close(c.entered)
	<-c.release
	return true
}

func (c *raceConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("raceConn: Prepare not supported")
}

func (c *raceConn) Close() error { return nil }

func (c *raceConn) Begin() (driver.Tx, error) { return nil, errors.New("raceConn: use BeginTx") }

func (c *raceConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	if c.block() {
		return nil, driverClosedErr
	}
	return nil, errors.New("raceConn: BeginTx not supported")
}

func (c *raceConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	if c.block() {
		return nil, driverClosedErr
	}
	return driver.RowsAffected(0), nil
}

func (c *raceConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.block() {
		return nil, driverClosedErr
	}
	return &emptyRows{}, nil
}

func (c *raceConn) Ping(context.Context) error { return nil }

// emptyRows 是零行结果集，仅供未被 arm 的查询路径使用。
type emptyRows struct{}

func (*emptyRows) Columns() []string         { return []string{"c"} }
func (*emptyRows) Close() error              { return nil }
func (*emptyRows) Next([]driver.Value) error { return io.EOF }

// newRaceProvider 用最小驱动构造一个 sqlstore.Provider（SQLite 方言）。
func newRaceProvider(t *testing.T) (*Provider, *raceConn) {
	t.Helper()
	conn := &raceConn{}
	db := sql.OpenDB(&raceConnector{conn: conn})
	p, err := New(db, SQLiteDialect)
	if err != nil {
		t.Fatalf("sqlstore.New: %v", err)
	}
	return p, conn
}

// TestCloseRaceNeverLeaksDriverError 逐个方法复现 check()/Close() 竞态窗口：
// 操作通过 check() 并阻塞在驱动里 → Close() 落地 → 放行驱动并让它返回
// driverClosedErr，然后断言调用方拿到的只能是 core.ErrClosed（errors.Is），
// 且错误文本里绝不出现驱动原始串。
//
// 表里覆盖 FullProvider 的全部方法（含批写与读写队列/zset）：任何一处漏用
// closedErr 归一，都会在此被确定性地钉住——不依赖调度时序，因此不会时好时坏。
func TestCloseRaceNeverLeaksDriverError(t *testing.T) {
	ctx := context.Background()
	// 每个用例都用独立的 Provider（Close 只能发生一次），闭包接收各自的 p。
	ops := []struct {
		name string
		call func(context.Context, *Provider) error
	}{
		{"Set", func(ctx context.Context, p *Provider) error { return p.Set(ctx, "k", []byte("v")) }},
		{"SetEx", func(ctx context.Context, p *Provider) error {
			return p.SetEx(ctx, "k", []byte("v"), 100)
		}},
		{"SetExAt", func(ctx context.Context, p *Provider) error {
			return p.SetExAt(ctx, "k", []byte("v"), core.NowUnix()+100)
		}},
		{"Get", func(ctx context.Context, p *Provider) error {
			_, _, err := p.Get(ctx, "k")
			return err
		}},
		{"Del", func(ctx context.Context, p *Provider) error { return p.Del(ctx, "k") }},
		{"Exists", func(ctx context.Context, p *Provider) error {
			_, err := p.Exists(ctx, "k")
			return err
		}},
		{"Incr", func(ctx context.Context, p *Provider) error {
			_, err := p.Incr(ctx, "k", 1)
			return err
		}},
		{"MGet", func(ctx context.Context, p *Provider) error {
			_, err := p.MGet(ctx, "k")
			return err
		}},
		{"Scan", func(ctx context.Context, p *Provider) error {
			_, err := p.Scan(ctx, "", "", 10)
			return err
		}},
		{"Expire", func(ctx context.Context, p *Provider) error { return p.Expire(ctx, "k", 100) }},
		{"ExpireAt", func(ctx context.Context, p *Provider) error {
			return p.ExpireAt(ctx, "k", core.NowUnix()+100)
		}},
		{"TTL", func(ctx context.Context, p *Provider) error {
			_, _, err := p.TTL(ctx, "k")
			return err
		}},
		{"QPush", func(ctx context.Context, p *Provider) error { return p.QPush(ctx, "q", []byte("v")) }},
		{"QPushFront", func(ctx context.Context, p *Provider) error {
			return p.QPushFront(ctx, "q", []byte("v"))
		}},
		{"QPop", func(ctx context.Context, p *Provider) error {
			_, _, err := p.QPop(ctx, "q")
			return err
		}},
		{"QPopBack", func(ctx context.Context, p *Provider) error {
			_, _, err := p.QPopBack(ctx, "q")
			return err
		}},
		{"QSize", func(ctx context.Context, p *Provider) error {
			_, err := p.QSize(ctx, "q")
			return err
		}},
		{"QFront", func(ctx context.Context, p *Provider) error {
			_, _, err := p.QFront(ctx, "q")
			return err
		}},
		{"QBack", func(ctx context.Context, p *Provider) error {
			_, _, err := p.QBack(ctx, "q")
			return err
		}},
		{"QRange", func(ctx context.Context, p *Provider) error {
			_, err := p.QRange(ctx, "q", 0, -1)
			return err
		}},
		{"ZSet", func(ctx context.Context, p *Provider) error { return p.ZSet(ctx, "z", "m", 1) }},
		{"ZGet", func(ctx context.Context, p *Provider) error {
			_, _, err := p.ZGet(ctx, "z", "m")
			return err
		}},
		{"ZDel", func(ctx context.Context, p *Provider) error { return p.ZDel(ctx, "z", "m") }},
		{"ZSize", func(ctx context.Context, p *Provider) error {
			_, err := p.ZSize(ctx, "z")
			return err
		}},
		{"ZRank", func(ctx context.Context, p *Provider) error {
			_, _, err := p.ZRank(ctx, "z", "m")
			return err
		}},
		{"ZRange", func(ctx context.Context, p *Provider) error {
			_, err := p.ZRange(ctx, "z", 0, -1)
			return err
		}},
		{"ZRangeByScore", func(ctx context.Context, p *Provider) error {
			_, err := p.ZRangeByScore(ctx, "z", 0, 10, 0, false)
			return err
		}},
		{"ZIncr", func(ctx context.Context, p *Provider) error {
			_, err := p.ZIncr(ctx, "z", "m", 1)
			return err
		}},
		{"ApplyBatch", func(ctx context.Context, p *Provider) error {
			return p.ApplyBatch(ctx, []core.BatchOp{{Kind: core.BatchSet, Key: "k", Value: []byte("v")}})
		}},
	}

	for _, op := range ops {
		op := op
		t.Run(op.name, func(t *testing.T) {
			p, conn := newRaceProvider(t)
			entered, release := conn.arm()

			result := make(chan error, 1)
			go func() { result <- op.call(ctx, p) }()

			// 等操作通过 check() 并真正进入驱动（即竞态窗口已经打开）。
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatalf("%s: 操作未在 10s 内进入驱动，测试夹具失效", op.name)
			}

			// 在途操作尚未返回时关闭：closed 置位、连接池关闭。
			closeDone := make(chan error, 1)
			go func() { closeDone <- p.Close() }()
			select {
			case err := <-closeDone:
				if err != nil {
					t.Fatalf("%s: Close: %v", op.name, err)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("%s: Close 未在 10s 内返回（在途操作阻塞了 database/sql 的 Close？）", op.name)
			}

			// 放行驱动：它返回 database/sql 的 "database is closed" 原始错误。
			close(release)
			select {
			case err := <-result:
				if err == nil {
					t.Fatalf("%s: 连接池已关闭且驱动返回错误，调用方却拿到 nil", op.name)
				}
				if !errors.Is(err, core.ErrClosed) {
					t.Fatalf("%s: 关闭竞态下泄漏了非哨兵错误: %v；want %v（底层驱动的 %q 必须经 closedErr 归一）",
						op.name, err, core.ErrClosed, driverClosedErr)
				}
				if strings.Contains(err.Error(), "database is closed") {
					t.Fatalf("%s: 错误文本仍含驱动原始串 %q: %v", op.name, "database is closed", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("%s: 放行驱动后 10s 内未返回", op.name)
			}
		})
	}
}
