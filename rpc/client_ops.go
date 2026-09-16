package rpc

import (
	"context"
	"fmt"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/rpc/codec"
)

// 本文件是 client 侧的命令实现。整体遵循两条规则：
//
//  1. **零语义加工**：只做参数编码与结果解码。任何"补偿性"逻辑（比如给缺失
//     的 key 造一个空值、把错误换成另一种）都会让远程与本地分叉。
//  2. **哨兵错误原样还原**：服务端用状态码区分的哨兵，这里用 errorForStatus
//     还原成同一个 core 错误对象，保证 errors.Is 在两条路径下结果一致。

// Set 写入 key。
func (p *Provider) Set(ctx context.Context, key string, value []byte) error {
	return p.callErr(ctx, mSet, []byte(key), value)
}

// SetEx 写入并设置 TTL。
func (p *Provider) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	return p.callErr(ctx, mSetEx, []byte(key), value, encInt(ttl))
}

// SetExAt 写入并设置绝对到期时刻（unix 秒）。
func (p *Provider) SetExAt(ctx context.Context, key string, value []byte, at int64) error {
	return p.callErr(ctx, mSetExAt, []byte(key), value, encInt(at))
}

// Get 读取 key。
func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	st, payload, err := p.call(ctx, []byte(mGet), []byte(key))
	if err != nil {
		return nil, false, err
	}
	switch st {
	case codec.StatusOK:
		if len(payload) == 0 {
			return nil, false, fmt.Errorf("rpc: GET returned ok with no payload")
		}
		return payload[0], true, nil
	case codec.StatusEmpty:
		return nil, false, nil
	}
	return nil, false, errorForStatus(st, payload)
}

// Del 删除 key。
func (p *Provider) Del(ctx context.Context, key string) error {
	return p.callErr(ctx, mDel, []byte(key))
}

// Exists 判断存在性。
func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	st, payload, err := p.call(ctx, []byte(mExists), []byte(key))
	if err != nil {
		return false, err
	}
	if st != codec.StatusOK || len(payload) == 0 {
		return false, errorForStatus(st, payload)
	}
	return decBool(payload[0])
}

// Incr 原子自增。
func (p *Provider) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	st, payload, err := p.call(ctx, []byte(mIncr), []byte(key), encInt(delta))
	if err != nil {
		return 0, err
	}
	if st != codec.StatusOK {
		return 0, errorForStatus(st, payload)
	}
	return decIntAt(payload, 0)
}

// MGet 批量读取。
func (p *Provider) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	args := make([][]byte, 0, len(keys)+1)
	args = append(args, []byte(mMGet))
	for _, k := range keys {
		args = append(args, []byte(k))
	}
	st, payload, err := p.call(ctx, args...)
	if err != nil {
		return nil, err
	}
	if st != codec.StatusOK {
		return nil, errorForStatus(st, payload)
	}
	if len(payload)%2 != 0 {
		return nil, fmt.Errorf("rpc: MGET payload has odd block count %d", len(payload))
	}
	out := make(map[string][]byte, len(payload)/2)
	for i := 0; i+1 < len(payload); i += 2 {
		out[string(payload[i])] = payload[i+1]
	}
	return out, nil
}

// Scan 区间扫描。
func (p *Provider) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	st, payload, err := p.call(ctx, []byte(mScan), []byte(start), []byte(end), encInt(int64(limit)))
	if err != nil {
		return nil, err
	}
	if st != codec.StatusOK {
		return nil, errorForStatus(st, payload)
	}
	if len(payload)%2 != 0 {
		return nil, fmt.Errorf("rpc: SCAN payload has odd block count %d", len(payload))
	}
	out := make([]core.KeyValue, 0, len(payload)/2)
	for i := 0; i+1 < len(payload); i += 2 {
		out = append(out, core.KeyValue{Key: string(payload[i]), Value: payload[i+1]})
	}
	return out, nil
}

// Expire 设置 TTL。
func (p *Provider) Expire(ctx context.Context, key string, ttl int64) error {
	return p.callErr(ctx, mExpire, []byte(key), encInt(ttl))
}

// ExpireAt 设置绝对到期时刻（unix 秒）。
func (p *Provider) ExpireAt(ctx context.Context, key string, at int64) error {
	return p.callErr(ctx, mExpireAt, []byte(key), encInt(at))
}

// TTL 读取剩余秒数。
func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	st, payload, err := p.call(ctx, []byte(mTTL), []byte(key))
	if err != nil {
		return 0, false, err
	}
	switch st {
	case codec.StatusOK:
		n, err := decIntAt(payload, 0)
		if err != nil {
			return 0, false, err
		}
		return n, true, nil
	case codec.StatusEmpty:
		return 0, false, nil
	}
	return 0, false, errorForStatus(st, payload)
}

// ---- Queue ----

// QPush 追加到队尾。
func (p *Provider) QPush(ctx context.Context, name string, value []byte) error {
	return p.callErr(ctx, mQPush, []byte(name), value)
}

// QPushFront 插入到队头。
func (p *Provider) QPushFront(ctx context.Context, name string, value []byte) error {
	return p.callErr(ctx, mQPushFr, []byte(name), value)
}

// QPop 取出队头。
func (p *Provider) QPop(ctx context.Context, name string) ([]byte, bool, error) {
	return p.popLike(ctx, mQPop, name)
}

// QPopBack 取出队尾。
func (p *Provider) QPopBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.popLike(ctx, mQPopBk, name)
}

// QSize 返回长度。
func (p *Provider) QSize(ctx context.Context, name string) (int64, error) {
	return p.intLike(ctx, mQSize, name)
}

// QFront 查看队头。
func (p *Provider) QFront(ctx context.Context, name string) ([]byte, bool, error) {
	return p.popLike(ctx, mQFront, name)
}

// QBack 查看队尾。
func (p *Provider) QBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.popLike(ctx, mQBack, name)
}

// QRange 返回区间内的元素（队头 → 队尾，保序）。
func (p *Provider) QRange(ctx context.Context, name string, start, stop int64) ([][]byte, error) {
	st, payload, err := p.call(ctx, []byte(mQRange), []byte(name), encInt(start), encInt(stop))
	if err != nil {
		return nil, err
	}
	if st != codec.StatusOK {
		return nil, errorForStatus(st, payload)
	}
	// payload 是原始字节块，逐个原样返回；空区间时 payload 为空，返回空切片。
	out := make([][]byte, 0, len(payload))
	for _, blk := range payload {
		out = append(out, blk)
	}
	return out, nil
}

// ---- ZSet ----

// ZSet 写入成员分数。
func (p *Provider) ZSet(ctx context.Context, name, key string, score int64) error {
	return p.callErr(ctx, mZSet, []byte(name), []byte(key), encInt(score))
}

// ZGet 读取成员分数。
func (p *Provider) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	st, payload, err := p.call(ctx, []byte(mZGet), []byte(name), []byte(key))
	if err != nil {
		return 0, false, err
	}
	switch st {
	case codec.StatusOK:
		n, err := decIntAt(payload, 0)
		if err != nil {
			return 0, false, err
		}
		return n, true, nil
	case codec.StatusEmpty:
		return 0, false, nil
	}
	return 0, false, errorForStatus(st, payload)
}

// ZDel 删除成员。
func (p *Provider) ZDel(ctx context.Context, name, key string) error {
	return p.callErr(ctx, mZDel, []byte(name), []byte(key))
}

// ZSize 返回成员数。
func (p *Provider) ZSize(ctx context.Context, name string) (int64, error) {
	return p.intLike(ctx, mZSize, name)
}

// ZRank 返回升序排名。
func (p *Provider) ZRank(ctx context.Context, name, key string) (int64, bool, error) {
	st, payload, err := p.call(ctx, []byte(mZRank), []byte(name), []byte(key))
	if err != nil {
		return 0, false, err
	}
	switch st {
	case codec.StatusOK:
		n, err := decIntAt(payload, 0)
		if err != nil {
			return 0, false, err
		}
		return n, true, nil
	case codec.StatusEmpty:
		return 0, false, nil
	}
	return 0, false, errorForStatus(st, payload)
}

// ZRange 返回区间成员（保序）。
func (p *Provider) ZRange(ctx context.Context, name string, start, stop int64) ([]core.ZItem, error) {
	st, payload, err := p.call(ctx, []byte(mZRange), []byte(name), encInt(start), encInt(stop))
	if err != nil {
		return nil, err
	}
	if st != codec.StatusOK {
		return nil, errorForStatus(st, payload)
	}
	if len(payload)%2 != 0 {
		return nil, fmt.Errorf("rpc: ZRANGE payload has odd block count %d", len(payload))
	}
	out := make([]core.ZItem, 0, len(payload)/2)
	for i := 0; i+1 < len(payload); i += 2 {
		score, err := decInt(payload[i+1])
		if err != nil {
			return nil, err
		}
		out = append(out, core.ZItem{Key: string(payload[i]), Score: score})
	}
	return out, nil
}

// ZRangeByScore 返回分数闭区间 [min, max] 内的成员（保序）。
//
// 编码与 ZRANGE 相同，另带 min/max/limit/desc 四个十进制文本参数；
// desc 编成 0/1（与其它 int 参数同一 decInt 口径），只翻转分数方向。
func (p *Provider) ZRangeByScore(ctx context.Context, name string, min, max int64, limit int, desc bool) ([]core.ZItem, error) {
	d := int64(0)
	if desc {
		d = 1
	}
	st, payload, err := p.call(ctx, []byte(mZRangeByScore), []byte(name),
		encInt(min), encInt(max), encInt(int64(limit)), encInt(d))
	if err != nil {
		return nil, err
	}
	if st != codec.StatusOK {
		return nil, errorForStatus(st, payload)
	}
	if len(payload)%2 != 0 {
		return nil, fmt.Errorf("rpc: ZRANGEBYSCORE payload has odd block count %d", len(payload))
	}
	out := make([]core.ZItem, 0, len(payload)/2)
	for i := 0; i+1 < len(payload); i += 2 {
		score, err := decInt(payload[i+1])
		if err != nil {
			return nil, err
		}
		out = append(out, core.ZItem{Key: string(payload[i]), Score: score})
	}
	return out, nil
}

// ZIncr 累加分数。
func (p *Provider) ZIncr(ctx context.Context, name, key string, delta int64) (int64, error) {
	st, payload, err := p.call(ctx, []byte(mZIncr), []byte(name), []byte(key), encInt(delta))
	if err != nil {
		return 0, err
	}
	if st != codec.StatusOK {
		return 0, errorForStatus(st, payload)
	}
	return decIntAt(payload, 0)
}

// ApplyBatch 把一批操作一次发到对端，由服务端调底层基座的 ApplyBatch。
//
// 整批只用一个往返，并且**按顺序**编码：底层基座若具备批内可见性
// （BatchComposed），远程经由此路径同样具备——因为服务端是把 ops 原样
// 交给底座的 ApplyBatch，中间没有任何拆分或重排。
func (p *Provider) ApplyBatch(ctx context.Context, ops []core.BatchOp) error {
	if len(ops) == 0 {
		return nil
	}
	args := make([][]byte, 0, len(ops)+1)
	args = append(args, []byte(mBatch))
	for _, op := range ops {
		k, err := wireKind(op.Kind)
		if err != nil {
			return err
		}
		args = append(args, encBatchOp(batchOpWire{
			Kind: k, Key: op.Key, Member: op.Member,
			Value: op.Value, TTL: op.TTL, Score: op.Score, Delta: op.Delta,
		}))
	}
	return p.callErrArgs(ctx, args)
}

// ---- 内部小工具 ----

// callErr 发一条命令，只关心成败（非 OK 即错误）。
func (p *Provider) callErr(ctx context.Context, cmd string, args ...[]byte) error {
	full := make([][]byte, 0, len(args)+1)
	full = append(full, []byte(cmd))
	full = append(full, args...)
	return p.callErrArgs(ctx, full)
}

func (p *Provider) callErrArgs(ctx context.Context, args [][]byte) error {
	st, payload, err := p.call(ctx, args...)
	if err != nil {
		return err
	}
	if st != codec.StatusOK {
		return errorForStatus(st, payload)
	}
	return nil
}

// popLike 处理"返回值 + ok"形态的命令（QPop/QFront/QBack）。
func (p *Provider) popLike(ctx context.Context, cmd, name string) ([]byte, bool, error) {
	st, payload, err := p.call(ctx, []byte(cmd), []byte(name))
	if err != nil {
		return nil, false, err
	}
	switch st {
	case codec.StatusOK:
		if len(payload) == 0 {
			return nil, false, fmt.Errorf("rpc: %s returned ok with no payload", cmd)
		}
		return payload[0], true, nil
	case codec.StatusEmpty:
		return nil, false, nil
	}
	return nil, false, errorForStatus(st, payload)
}

// intLike 处理"返回 int64"的命令（QSize/ZSize）。
func (p *Provider) intLike(ctx context.Context, cmd, name string) (int64, error) {
	st, payload, err := p.call(ctx, []byte(cmd), []byte(name))
	if err != nil {
		return 0, err
	}
	if st != codec.StatusOK {
		return 0, errorForStatus(st, payload)
	}
	return decIntAt(payload, 0)
}
