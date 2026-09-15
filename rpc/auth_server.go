package rpc

import (
	"crypto/subtle"

	"github.com/RelicOfTesla/kvdb/rpc/codec"
)

// handleAuth 处理一条认证命令，返回更新后的 (已认证, 待验证 nonce)。
//
// 三种模式的服务端行为：
//
//	AuthNone      —— 不接受任何 AUTH 命令（回 auth_failed），连接直接算已认证
//	AuthPlain     —— AUTH <password>，定长比较
//	AuthChallenge —— AUTHCHAL（下发 nonce）→ AUTHCHAL <hex HMAC>（校验）
//
// 两条与安全相关的约定：
//
//  1. 已认证的连接再次发 AUTH 不报错，按"重新认证"处理（幂等重放无害）。
//  2. 挑战模式的 nonce **用后即弃**：校验成功或失败都清空，且重发
//     AUTHCHAL 会生成新 nonce。因此捕获到的应答无法重放。
func (s *Server) handleAuth(tr *transport, cmd string, args [][]byte, authed bool, challenge []byte) (bool, []byte) {
	if s.cfg.Auth == AuthNone {
		// 服务端没开认证，却在发 AUTH：属于客户端配置错误，明确报错而不是静默放行。
		_ = tr.cdc.WriteResponse(tr.c, codec.StatusAuthFailed, []byte("server does not require authentication"))
		return authed, nil
	}

	switch cmd {
	case cmdAuthPlain:
		if s.cfg.Auth != AuthPlain {
			_ = tr.cdc.WriteResponse(tr.c, codec.StatusAuthFailed, []byte("plain auth is not enabled on this server"))
			return authed, nil
		}
		if len(args) != 1 {
			_ = tr.cdc.WriteResponse(tr.c, codec.StatusError, []byte("rpc: AUTH expects 1 arg (password)"))
			return authed, nil
		}
		if constantTimeEqualString(string(args[0]), s.cfg.Password) {
			_ = tr.cdc.WriteResponse(tr.c, codec.StatusOK)
			return true, nil
		}
		_ = tr.cdc.WriteResponse(tr.c, codec.StatusAuthFailed, []byte("invalid password"))
		return false, nil

	case cmdAuthChallenge:
		if s.cfg.Auth != AuthChallenge {
			_ = tr.cdc.WriteResponse(tr.c, codec.StatusAuthFailed, []byte("challenge auth is not enabled on this server"))
			return authed, nil
		}
		switch len(args) {
		case 0:
			// 第一步：下发 nonce
			nonce, err := newChallenge()
			if err != nil {
				_ = tr.cdc.WriteResponse(tr.c, codec.StatusError, []byte(err.Error()))
				return authed, nil
			}
			_ = tr.cdc.WriteResponse(tr.c, codec.StatusOK, encodeHex(nonce))
			return authed, nonce
		case 1:
			// 第二步：校验应答。没有待验证 nonce 说明客户端跳过了第一步。
			if challenge == nil {
				_ = tr.cdc.WriteResponse(tr.c, codec.StatusAuthFailed, []byte("no pending challenge; send AUTHCHAL first"))
				return authed, nil
			}
			got, err := decodeHex(args[0])
			if err != nil {
				_ = tr.cdc.WriteResponse(tr.c, codec.StatusAuthFailed, []byte("malformed challenge response"))
				return false, nil
			}
			ok := verifyChallenge(s.cfg.Password, challenge, got)
			challenge = nil // 一次性：成功失败都不再复用
			if !ok {
				_ = tr.cdc.WriteResponse(tr.c, codec.StatusAuthFailed, []byte("invalid challenge response"))
				return false, nil
			}
			_ = tr.cdc.WriteResponse(tr.c, codec.StatusOK)
			return true, nil
		default:
			_ = tr.cdc.WriteResponse(tr.c, codec.StatusError, []byte("rpc: AUTHCHAL expects 0 or 1 arg"))
			return authed, challenge
		}
	}
	return authed, challenge
}

// constantTimeEqualString 定长比较，避免口令校验泄漏前缀信息。
// 长度不同时仍走一次比较再返回，保证耗时与"长度是否相等"无关。
func constantTimeEqualString(a, b string) bool {
	if len(a) != len(b) {
		// 与自身比较一次，消耗与等长比较同量级的时间。
		subtle.ConstantTimeCompare([]byte(a), []byte(a))
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
