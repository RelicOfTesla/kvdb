// 时钟注入点：所有基座一律通过 Now / NowUnix 取"当前时刻"，不直接调 time.Now。
// 测试里替换 core.Now（或 kvdb.Now）即可确定性触发过期边界，无需 sleep 真实秒数。
package core

import "time"

// Now 是可替换的当前时间来源，默认 time.Now。
//
// 约定：基座每次公开操作只采样一次（把 now 传进同一事务/同一持锁区间），
// 避免同一次调用内前后取到不同时刻导致语义漂移。
var Now = time.Now

// NowUnix 是 Now().Unix() 的简写，用于以"秒"为粒度的 TTL/expire_at 计算。
func NowUnix() int64 { return Now().Unix() }

// SweepInterval 是写路径"顺带回收过期条目"的最小间隔（秒）。设为 0 可让每次写
// 都触发回收（测试用），过大则过期数据驻留更久。各基座共用此值以保持行为一致。
var SweepInterval int64 = 60
