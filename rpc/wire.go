package rpc

import (
	"fmt"
	"strconv"
)

// 方法名。用短小的 ASCII 命令名而不是 "KvProvider.Set" 这类长标识：
// 每个请求都带方法名，缩短它能省下可观的字节，也让它更接近 redis/ssdb 的观感。
const (
	mSet      = "SET"
	mSetEx    = "SETEX"
	mSetExAt  = "SETEXAT"
	mGet      = "GET"
	mDel      = "DEL"
	mExists   = "EXISTS"
	mIncr     = "INCR"
	mMGet     = "MGET"
	mScan     = "SCAN"
	mExpire   = "EXPIRE"
	mExpireAt = "EXPIREAT"
	mTTL      = "TTL"
	mBatch    = "BATCH"
	mQPush    = "QPUSH"
	mQPushFr  = "QPUSHFRONT"
	mQPop     = "QPOP"
	mQPopBk   = "QPOPBACK"
	mQSize    = "QSIZE"
	mQFront   = "QFRONT"
	mQBack    = "QBACK"
	mQRange   = "QRANGE"
	mZSet     = "ZSET"
	mZGet     = "ZGET"
	mZDel     = "ZDEL"
	mZSize    = "ZSIZE"
	mZRank    = "ZRANK"
	mZRange   = "ZRANGE"
	// mZRangeByScore 比 ZRANGE 多 min/max/limit/desc 四个参数：
	// min/max/limit 是十进制文本（与 ZRANGE 的 start/stop 同规），
	// desc 也按十进制文本编码（0/1），与 decInt 的唯一 int 编码一致。
	mZRangeByScore = "ZRANGEBYSCORE"
	mZIncr         = "ZINCR"
	mCaps          = "CAPS"
	mHello         = "HELLO"
)

// 编码约定（与 codec 无关，纯粹是"参数怎么摆"）：
//
//   - 字符串键：UTF-8 原文，直接一块
//   - value：原始字节，直接一块（二进制安全由 codec 保证）
//   - int64：十进制文本。选文本而非定长二进制，是为了让请求在
//     `nc`/redis-cli 里可读、可手打；整数解析开销相对一次网络往返可忽略。
//   - bool/ok：只作为**应答**出现，约定见下
//   - []byte 可选值（如 Get）：命中时首块为值，未命中走 StatusEmpty（无块）
//   - map（MGet）：块序列 [k1 v1 k2 v2 ...]，顺序不保证
//   - []KeyValue（Scan）：块序列 [k1 v1 k2 v2 ...] 且**保序**
//   - []ZItem（ZRange）：块序列 [member1 score1 member2 score2 ...] 且**保序**
//   - []ZItem（ZRangeByScore）：与 ZRange **同一编码**（同样保序）；只是请求多带
//     min、max（十进制文本的分数闭区间）、limit（十进制文本，<=0 表示不限）、
//     desc（十进制文本 0/1，只翻转分数方向，不改 min<=max 的参数含义）
//   - [][]byte（QRange）：块序列 [v1 v2 v3 ...] 且**保序**
//   - 可选 int64（如 TTL、ZGet、ZRank）：命中时首块为十进制值，未命中走 StatusEmpty
//   - BatchOp：单块内自描述序列，见下方编码表

// encInt 把 int64 编成十进制文本块。
func encInt(v int64) []byte { return strconv.AppendInt(nil, v, 10) }

// decInt 解析十进制文本块。
func decInt(b []byte) (int64, error) {
	v, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("rpc: bad int64 %q: %w", truncateBytes(b), err)
	}
	return v, nil
}

func decIntAt(payload [][]byte, i int) (int64, error) {
	if i >= len(payload) {
		return 0, fmt.Errorf("rpc: missing int64 at position %d", i)
	}
	return decInt(payload[i])
}

// ---- BatchOp 的线格式 ----
//
// 一条 BatchOp 编成**单个块**：`kind:field:field:...`，字段按 kind 取用，
// 其中只有 Value 是裸字节（二进制安全），因此它必须放在最后一段；
// Key/Member 是字符串，但为了避免定界符冲突，统一做 hex 编码。
//
//	SET      -> 1:<hex key>:<value>
//	SETEX    -> 2:<hex key>:<ttl>:<value>
//	DEL      -> 3:<hex key>
//	EXPIRE   -> 4:<hex key>:<ttl>
//	QPUSH    -> 5:<hex name>:<value>
//	QPUSHFRONT -> 6:<hex name>:<value>
//	ZSET     -> 7:<hex name>:<hex member>:<score>
//	ZDEL     -> 8:<hex name>:<hex member>
//	ZINCR    -> 9:<hex name>:<hex member>:<delta>
//
// 选这一形态而不是"每个字段一块"，是因为批内的操作条数不定，单块自描述
// 让 COUNT + 每块一条 op 的报文结构保持简单（一条 op 出错的定位也更直接）。
func encBatchOp(op batchOpWire) []byte {
	var b []byte
	b = strconv.AppendInt(b, int64(op.Kind), 10)
	switch op.Kind {
	case kindSet:
		b = append(b, ':')
		b = appendHexField(b, op.Key)
		b = append(b, ':')
		b = append(b, op.Value...)
	case kindSetEx:
		b = append(b, ':')
		b = appendHexField(b, op.Key)
		b = append(b, ':')
		b = strconv.AppendInt(b, op.TTL, 10)
		b = append(b, ':')
		b = append(b, op.Value...)
	case kindDel:
		b = append(b, ':')
		b = appendHexField(b, op.Key)
	case kindExpire:
		b = append(b, ':')
		b = appendHexField(b, op.Key)
		b = append(b, ':')
		b = strconv.AppendInt(b, op.TTL, 10)
	case kindQPush, kindQPushFront:
		b = append(b, ':')
		b = appendHexField(b, op.Key)
		b = append(b, ':')
		b = append(b, op.Value...)
	case kindZSet:
		b = append(b, ':')
		b = appendHexField(b, op.Key)
		b = append(b, ':')
		b = appendHexField(b, op.Member)
		b = append(b, ':')
		b = strconv.AppendInt(b, op.Score, 10)
	case kindZDel:
		b = append(b, ':')
		b = appendHexField(b, op.Key)
		b = append(b, ':')
		b = appendHexField(b, op.Member)
	case kindZIncr:
		b = append(b, ':')
		b = appendHexField(b, op.Key)
		b = append(b, ':')
		b = appendHexField(b, op.Member)
		b = append(b, ':')
		b = strconv.AppendInt(b, op.Delta, 10)
	}
	return b
}

func appendHexField(b []byte, s string) []byte {
	const hexdigits = "0123456789abcdef"
	for i := 0; i < len(s); i++ {
		b = append(b, hexdigits[s[i]>>4], hexdigits[s[i]&0xf])
	}
	return b
}

func parseHexField(b []byte) (string, error) {
	if len(b)%2 != 0 {
		return "", fmt.Errorf("rpc: odd-length hex field %q", truncateBytes(b))
	}
	out := make([]byte, len(b)/2)
	for i := 0; i < len(out); i++ {
		hi, ok1 := hexVal(b[2*i])
		lo, ok2 := hexVal(b[2*i+1])
		if !ok1 || !ok2 {
			return "", fmt.Errorf("rpc: bad hex field %q", truncateBytes(b))
		}
		out[i] = hi<<4 | lo
	}
	return string(out), nil
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// decBatchOp 解析单个块。
func decBatchOp(blk []byte) (batchOpWire, error) {
	var op batchOpWire
	kind, i, err := readUintField(blk, 0)
	if err != nil {
		return op, err
	}
	op.Kind = batchKind(kind)

	switch op.Kind {
	case kindSet, kindQPush, kindQPushFront:
		// <kind>:<hex key>:<value 到末尾>
		key, ni, err := readHexField(blk, i)
		if err != nil {
			return op, err
		}
		op.Key = key
		op.Value = blk[ni:]

	case kindSetEx, kindExpire:
		key, ni, err := readHexField(blk, i)
		if err != nil {
			return op, err
		}
		op.Key = key
		if op.Kind == kindExpire {
			ttl, _, err := readIntField(blk, ni)
			if err != nil {
				return op, err
			}
			op.TTL = ttl
			break
		}
		// SETEX: <kind>:<hex key>:<ttl>:<value 到末尾>
		ttl, ti, err := readIntField(blk, ni)
		if err != nil {
			return op, err
		}
		op.TTL = ttl
		op.Value = blk[ti:]

	case kindDel:
		key, _, err := readHexField(blk, i)
		if err != nil {
			return op, err
		}
		op.Key = key

	case kindZSet:
		name, ni, err := readHexField(blk, i)
		if err != nil {
			return op, err
		}
		member, mi, err := readHexField(blk, ni)
		if err != nil {
			return op, err
		}
		score, _, err := readIntField(blk, mi)
		if err != nil {
			return op, err
		}
		op.Key, op.Member, op.Score = name, member, score

	case kindZDel, kindZIncr:
		name, ni, err := readHexField(blk, i)
		if err != nil {
			return op, err
		}
		member, mi, err := readHexField(blk, ni)
		if err != nil {
			return op, err
		}
		op.Key, op.Member = name, member
		if op.Kind == kindZIncr {
			delta, _, err := readIntField(blk, mi)
			if err != nil {
				return op, err
			}
			op.Delta = delta
		}

	default:
		return op, fmt.Errorf("rpc: unknown batch op kind %d", op.Kind)
	}
	return op, nil
}

// 字段读取的统一约定：函数接收"字段起点"，返回 **分隔符之后**的位置
// （即下一个字段的起点），使调用方无需自己做 ±1 换算。行格式字段若
// 已到末尾则返回 len(b)。这个约定必须一致，否则每个字段都会错位一个字节。
func readUintField(b []byte, i int) (uint64, int, error) {
	j := i
	for j < len(b) && b[j] != ':' {
		j++
	}
	if j == i {
		return 0, 0, fmt.Errorf("rpc: empty numeric field at %d", i)
	}
	v, err := strconv.ParseUint(string(b[i:j]), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("rpc: bad numeric field %q: %w", truncateBytes(b[i:j]), err)
	}
	return v, skipColon(b, j), nil
}

func readIntField(b []byte, i int) (int64, int, error) {
	j := i
	for j < len(b) && b[j] != ':' {
		j++
	}
	if j == i {
		return 0, 0, fmt.Errorf("rpc: empty int field at %d", i)
	}
	v, err := strconv.ParseInt(string(b[i:j]), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("rpc: bad int field %q: %w", truncateBytes(b[i:j]), err)
	}
	return v, skipColon(b, j), nil
}

// skipColon 跳过字段末尾的 ':'（若在末尾则停在 len(b)）。
func skipColon(b []byte, j int) int {
	if j < len(b) && b[j] == ':' {
		return j + 1
	}
	return j
}

// readHexField 读一个 hex 字段，返回解码后的字符串与下一个字段的起点。
func readHexField(b []byte, i int) (string, int, error) {
	j := i
	for j < len(b) && b[j] != ':' {
		j++
	}
	s, err := parseHexField(b[i:j])
	if err != nil {
		return "", 0, err
	}
	return s, skipColon(b, j), nil
}

func truncateBytes(b []byte) string {
	const max = 24
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "..."
}
