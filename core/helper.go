// 参数校验与其它跨基座共享的小工具。
//
// 单独成文件（而不是塞进 provider.go）：provider.go 只放**契约本身**（接口、哨兵
// 错误、能力声明），这类"各基座实现里要显式调用"的辅助函数放这里，便于一眼看清
// 有哪些跨基座约定。
package core

import "math"

// CheckKey 校验 key/队列名/zset 名非空，供各基座在实现里显式调用。
//
// 为什么放在 core 而不是适配层（DB）校验：契约是**基座**要满足的接口，直接使用
// Provider（绕过 DB）时同样必须成立；只在 DB 层拦会让契约形同虚设。
// 共享此函数只是为了判定口径一致，调用与否仍由各基座自己负责。
//
// 契约把空 key 定义为非法且**读写一致拒绝**：只拒写不拒读会造成
// "写不进去却读得到"的自相矛盾（真实 SSDB 对空 key 返回 ok 却静默丢弃写入）。
func CheckKey(key string) error {
	if key == "" {
		return ErrInvalidKey
	}
	return nil
}

// CheckKeys 是 CheckKey 的多参数版本（zset 的 name+member 等需要同时校验）。
func CheckKeys(keys ...string) error {
	for _, k := range keys {
		if k == "" {
			return ErrInvalidKey
		}
	}
	return nil
}

// AddTTL 返回 now+ttl 的饱和和：ttl 大到溢出 int64 时钳制到 MaxInt64，
// 避免各基座把 expire_at 包绕成负数（键立即过期或永不过期的分歧）。
func AddTTL(now, ttl int64) int64 {
	if ttl > 0 && now > math.MaxInt64-ttl {
		return math.MaxInt64
	}
	return now + ttl
}

// MaxRelativeTTL 是"相对秒数"可安全下发的上限。
//
// 服务端基座（如 SSDB）收到的是**相对秒数**，由服务端自行做 now+ttl；若客户端
// 直接下发 MaxInt64，服务端相加会溢出成负数，键变成"立即过期"——与契约要求的
// 饱和语义（见 AddTTL）相反。因此在客户端就把相对 TTL 钳到该上限：
// 约 292 年，足够表达"永不过期"，又不至于让服务端相加溢出（前提是服务端时钟
// 不会超过约 292 年后仍使用 int64 秒）。
const MaxRelativeTTL int64 = math.MaxInt64 / 2

// ClampTTL 把相对 TTL 钳到 [1, MaxRelativeTTL]。
// 调用方应已拒绝 ttl<=0（契约要求 ErrInvalidTTL），此函数只负责上界。
func ClampTTL(ttl int64) int64 {
	if ttl > MaxRelativeTTL {
		return MaxRelativeTTL
	}
	if ttl < 1 {
		return 1
	}
	return ttl
}
