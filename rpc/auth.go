package rpc

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/RelicOfTesla/kvdb/rpc/codec"
)

// AuthMode 是 c/s 之间的认证模式。它是**协议自己的**认证，与底座自身的
// 认证（如 ssdb server.auth、mysql 用户口令）无关——那些在 server 侧的
// 基座连接里已经处理完了，client 不需要也不应该重复承担。
type AuthMode uint8

const (
	// AuthNone 不做 c/s 认证。默认模式（安全的缺省），适合本机/内网。
	AuthNone AuthMode = iota
	// AuthPlain 明文口令：客户端直接发送口令，服务端定长比较。
	// **默认不打开**，因为口令在网络上裸奔（即便套了 TLS 也只是把风险降到链路层）。
	// 仅用于"已经用 TLS 或走 unix socket"的简单场景。
	AuthPlain
	// AuthChallenge 挑战-响应：服务端下发随机 nonce，客户端回 HMAC-SHA256
	// 证明持有口令而**不发送口令本身**；nonce 一次性，重放无效。
	// 这是需要认证时的推荐模式。
	AuthChallenge
)

func (m AuthMode) String() string {
	switch m {
	case AuthNone:
		return "none"
	case AuthPlain:
		return "plain"
	case AuthChallenge:
		return "challenge"
	}
	return "mode(" + strconv.Itoa(int(m)) + ")"
}

// ParseAuthMode 解析配置/URI 里的模式名。空串按 AuthNone 处理。
func ParseAuthMode(s string) (AuthMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "none", "off", "disable":
		return AuthNone, nil
	case "plain", "password":
		return AuthPlain, nil
	case "challenge", "hmac", "challenge-response":
		return AuthChallenge, nil
	}
	return 0, fmt.Errorf("rpc: unknown auth mode %q (want none|plain|challenge)", s)
}

// 认证握手用的命令名。它们与数据命令共用同一 codec 与同一条连接，
// 由 server 的分发器拦在鉴权闸门之前。
const (
	cmdAuthPlain     = "AUTH"
	cmdAuthChallenge = "AUTHCHAL"
	cmdPing          = "PING"
	cmdHello         = mHello
)

// authOK 是挑战握手中服务端期望的应答长度（hex 编码的 HMAC-SHA256）。
const challengeKeyLen = sha256.Size

// newChallenge 生成一个一次性 nonce。长度 32 字节：足以让离线穷举无意义，
// 又不至于让每次建连多出明显的字节数。
func newChallenge() ([]byte, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("rpc: generate challenge: %w", err)
	}
	return b, nil
}

// challengeResponse 计算 nonce 对应的应答：HMAC-SHA256(key=password, msg=nonce)。
//
// 选 HMAC 而不是裸 SHA256(password||nonce)：后者存在长度扩展攻击面，
// 而 HMAC 是为此设计的标准构造，标准库直接提供。
//
// 口令先做一次 SHA256 再作为 HMAC 密钥，好处是任意长度的口令都被压成
// 固定 32 字节密钥（HMAC 本身也接受长密钥，这里只是让两条分支一致）。
func challengeResponse(password string, nonce []byte) []byte {
	key := sha256.Sum256([]byte(password))
	mac := hmac.New(sha256.New, key[:])
	mac.Write(nonce)
	return mac.Sum(nil)
}

// verifyChallenge 以定长比较校验应答，避免按字节提前返回带来的时序侧信道。
func verifyChallenge(password string, nonce, got []byte) bool {
	want := challengeResponse(password, nonce)
	return subtle.ConstantTimeCompare(want, got) == 1
}

// encodeHex 用于把二进制应答放进文本友好的 RPC 参数里
// （RESP codec 的参数虽有转义、能承载裸字节，但握手用 hex 让抓包与手测都更直观）。
func encodeHex(b []byte) []byte {
	out := make([]byte, hex.EncodedLen(len(b)))
	hex.Encode(out, b)
	return out
}

func decodeHex(b []byte) ([]byte, error) {
	out := make([]byte, hex.DecodedLen(len(b)))
	n, err := hex.Decode(out, b)
	if err != nil {
		return nil, fmt.Errorf("rpc: bad hex payload: %w", err)
	}
	return out[:n], nil
}

// ---- 客户端侧握手 ----

// clientAuth 与 server 完成认证握手。
//
// 三种模式对应三种往返：
//
//	AuthNone      —— 不发送任何命令
//	AuthPlain     —— AUTH <password>，1 次往返
//	AuthChallenge —— AUTHCHAL（取 nonce）→ 计算 HMAC → AUTHCHAL <hex 应答>，2 次往返
//
// 握手失败返回带 ErrAuthFailed 的错误（可用 errors.Is 判定）。
func clientAuth(rt *transport, mode AuthMode, password string) error {
	switch mode {
	case AuthNone:
		return nil
	case AuthPlain:
		st, payload, err := rt.call([]byte(cmdAuthPlain), []byte(password))
		if err != nil {
			return err
		}
		if st == codec.StatusOK {
			return nil
		}
		return authError(st, payload)
	case AuthChallenge:
		st, payload, err := rt.call([]byte(cmdAuthChallenge))
		if err != nil {
			return err
		}
		if st != codec.StatusOK || len(payload) == 0 {
			return authError(st, payload)
		}
		nonce, err := decodeHex(payload[0])
		if err != nil {
			return err
		}
		resp := encodeHex(challengeResponse(password, nonce))
		st, payload, err = rt.call([]byte(cmdAuthChallenge), resp)
		if err != nil {
			return err
		}
		if st == codec.StatusOK {
			return nil
		}
		return authError(st, payload)
	}
	return fmt.Errorf("rpc: unsupported auth mode %d", mode)
}
