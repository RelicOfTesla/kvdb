// Package codec 定义 kvdb RPC 的报文编解码抽象，以及内置的 RESP 实现。
//
// 抽象存在的理由：c/s 之间的帧格式属于"接缝"，不同部署对它的诉求不同——
// 调试期希望拿 redis-cli / nc 直接手打请求，生产期可能要求与既有协议栈一致。
// 把编解码收敛到 Codec 接口后，server 与 client 各持一个实现即可整体替换，
// 认证、握手、调用分发全部复用同一口径，不会出现两端格式脱节。
//
// 内置实现：
//
//	codec.RESP       —— 默认。仿 Redis 序列化协议（RESP2），见 resp.go
//	codec.Binary     —— 长度前缀二进制帧，见 binary.go
//	codec.TextProto  —— 类 SSDB 的文本协议（长度前缀 + 空行结束；命令与状态属
//	                    kvdb，**不与 SSDB 互通**），见 textproto.go
//
// 自定义实现只需满足本接口；两端必须装配同一实现。
package codec

import (
	"errors"
	"fmt"
	"io"
)

// ErrShortRead 表示连接在报文读全之前关闭。
var ErrShortRead = errors.New("kvdb/rpc/codec: unexpected end of stream")

// ErrTooLarge 表示某个长度字段超出 MaxBlobBytes，属于协议级拒绝
// （对端异常或恶意），读取侧不应按声明值直接分配内存。
var ErrTooLarge = errors.New("kvdb/rpc/codec: declared length exceeds limit")

// MaxBlobBytes 是单个二进制块的上限（含 value，以及批操作里的每个 value）。
// 64 MiB 与 ssdb 基座的上限取值一致：足够容纳常见的大 value，
// 又不至于让一个坏长度字段就能触发 GB 级分配。
const MaxBlobBytes = 1 << 26

// MaxArgs 是一条命令内参数个数上限（含命令名），防御超长参数列表。
const MaxArgs = 1 << 16

// Codec 是报文编解码器。一个实例只服务于一条连接，**不是**并发安全的。
//
// Request 写出一条命令：args[0] 是命令名（ASCII，不含空白），其余为参数，
// 参数按 []byte 原样传输，因此二进制安全。
//
// Response 写出或读入一条应答：status 是应答类别（见 Status* 常量），
// payload 是负载块。读入侧必须能识别并解析**本实现自己写出的**全部形态。
type Codec interface {
	// Name 返回实现名（诊断用，如 "resp" / "binary"）。
	Name() string
	// WriteRequest 写出一条命令并 Flush。
	WriteRequest(w io.Writer, args [][]byte) error
	// ReadRequest 读入一条命令。
	ReadRequest(r io.Reader) ([][]byte, error)
	// WriteResponse 写出一条应答并 Flush。
	WriteResponse(w io.Writer, status Status, payload ...[]byte) error
	// ReadResponse 读入一条应答。
	ReadResponse(r io.Reader) (Status, [][]byte, error)
}

// Status 是应答类别，独立于具体线格式：不同 codec 用各自的符号表达同一语义。
type Status uint8

const (
	// StatusOK 表示成功，payload 为结果集（可空）。
	StatusOK Status = iota + 1
	// StatusEmpty 表示"查无此项"：对应 Get 未命中、队列为空这类
	// 正常但无值的语义，客户端据此产生 ok=false 而非错误。
	StatusEmpty
	// StatusError 表示命令执行失败，payload 为错误消息（通常单块）。
	StatusError
	// StatusUnsupported 表示底层基座未实现该能力，客户端映射为 core.ErrUnsupported。
	StatusUnsupported
	// StatusClosed 表示底层基座已关闭，客户端映射为 core.ErrClosed。
	StatusClosed
	// StatusNotInteger 表示 Incr 遇到非十进制整数值，映射为 core.ErrNotInteger。
	StatusNotInteger
	// StatusInvalidTTL 表示 TTL<=0，映射为 core.ErrInvalidTTL。
	StatusInvalidTTL
	// StatusInvalidKey 表示 key/队列名/zset 名/成员为空串，
	// 映射为 core.ErrInvalidKey。
	//
	// 单独给一个状态而不是落到通用 StatusError，是因为 StatusError 过线后
	// 客户端只能还原出 errors.New(消息文本) —— 文本虽然与哨兵一字不差，
	// 但**哨兵身份丢失**，errors.Is(err, core.ErrInvalidKey) 为 false，
	// 直接破坏"远程基座与本地基座行为一致"的合同。这里与 StatusInvalidTTL /
	// StatusNotInteger 同一手法：给需要判等的哨兵各配一个专属状态。
	StatusInvalidKey
	// StatusNotFound 表示目标不存在且该命令以错误形态上报，映射为 core.ErrNotFound。
	StatusNotFound
	// StatusAuthRequired 表示命令在认证完成之前被拒。
	StatusAuthRequired
	// StatusAuthFailed 表示认证失败（凭据错/挑战应答不匹配）。
	StatusAuthFailed
)

// String 便于日志与测试断言。
func (s Status) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusEmpty:
		return "empty"
	case StatusError:
		return "error"
	case StatusUnsupported:
		return "unsupported"
	case StatusClosed:
		return "closed"
	case StatusNotInteger:
		return "not_integer"
	case StatusInvalidTTL:
		return "invalid_ttl"
	case StatusInvalidKey:
		return "invalid_key"
	case StatusNotFound:
		return "not_found"
	case StatusAuthRequired:
		return "auth_required"
	case StatusAuthFailed:
		return "auth_failed"
	}
	return fmt.Sprintf("status(%d)", uint8(s))
}

// valid 用于读取侧校验，挡住越界状态字节被当作正常应答。
func (s Status) valid() bool { return s >= StatusOK && s <= StatusAuthFailed }
