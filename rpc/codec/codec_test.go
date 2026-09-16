package codec_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/RelicOfTesla/kvdb/rpc/codec"
)

// 内置 codec 必须能原样往返任意字节，包括空参数与二进制脏值——
// 这正是"参数不能用裸文本分隔"的起因（空参数会被当成报文结束）。

func newCodec(name string, r io.Reader, w io.Writer) codec.Codec {
	switch name {
	case "binary":
		return codec.NewBinary(r, w)
	case "textproto":
		return codec.NewTextProto(r, w)
	default:
		return codec.NewRESP(r, w)
	}
}

func codecNames() []string { return []string{"resp", "binary", "textproto"} }

// TestRequestRoundTrip 覆盖空参数、二进制值、CRLF、以及参数个数为 0/1 的边界。
func TestRequestRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		args [][]byte
	}{
		{"单命令名", [][]byte{[]byte("PING")}},
		{"普通参数", [][]byte{[]byte("SET"), []byte("k"), []byte("v")}},
		{"空参数", [][]byte{[]byte("SCAN"), {}, {}, []byte("100")}},
		{"全空参数", [][]byte{[]byte("X"), {}, {}}},
		{"空命令名", [][]byte{{}, []byte("a")}},
		{"二进制值", [][]byte{[]byte("SET"), []byte("k"), {0x00, 0xff, 0x0a, 0x0d, '$', '*', ':'}}},
		{"参数含 CRLF", [][]byte{[]byte("SET"), []byte("k"), []byte("a\r\nb")}},
		{"参数含裸 LF", [][]byte{[]byte("SET"), []byte("k"), []byte("a\nb")}},
		{"参数含冒号", [][]byte{[]byte("SET"), []byte("a:b"), []byte("c:d")}},
		{"长参数", [][]byte{[]byte("SET"), bytes.Repeat([]byte("x"), 70000)}},
	}
	for _, name := range codecNames() {
		for _, tc := range cases {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				var buf bytes.Buffer
				c := newCodec(name, nil, &buf)
				if err := c.WriteRequest(&buf, tc.args); err != nil {
					t.Fatalf("WriteRequest: %v", err)
				}
				got, err := newCodec(name, &buf, io.Discard).ReadRequest(&buf)
				if err != nil {
					t.Fatalf("ReadRequest: %v", err)
				}
				if len(got) != len(tc.args) {
					t.Fatalf("参数个数 = %d, want %d", len(got), len(tc.args))
				}
				for i := range got {
					if !bytes.Equal(got[i], tc.args[i]) {
						t.Fatalf("参数[%d] = %q, want %q", i, got[i], tc.args[i])
					}
				}
			})
		}
	}
}

// TestResponseRoundTrip 覆盖各状态与多块负载。
func TestResponseRoundTrip(t *testing.T) {
	statuses := []codec.Status{
		codec.StatusOK, codec.StatusEmpty, codec.StatusError,
		codec.StatusUnsupported, codec.StatusClosed, codec.StatusNotInteger,
		codec.StatusInvalidTTL, codec.StatusNotFound,
		codec.StatusAuthRequired, codec.StatusAuthFailed,
	}
	payloads := [][][]byte{
		nil,
		{[]byte("single")},
		{[]byte("k1"), []byte("v1"), []byte("k2"), {}},
		{{0x00, 0x01, 0xff}, []byte("x\ry\nz")},
	}
	for _, name := range codecNames() {
		for _, st := range statuses {
			for _, pl := range payloads {
				var buf bytes.Buffer
				c := newCodec(name, nil, &buf)
				if err := c.WriteResponse(&buf, st, pl...); err != nil {
					t.Fatalf("WriteResponse: %v", err)
				}
				gotSt, gotPl, err := newCodec(name, &buf, io.Discard).ReadResponse(&buf)
				if err != nil {
					t.Fatalf("ReadResponse: %v", err)
				}
				if gotSt != st {
					t.Fatalf("status = %v, want %v", gotSt, st)
				}
				if len(gotPl) != len(pl) {
					t.Fatalf("块数 = %d, want %d", len(gotPl), len(pl))
				}
				for i := range gotPl {
					if !bytes.Equal(gotPl[i], pl[i]) {
						t.Fatalf("块[%d] = %q, want %q", i, gotPl[i], pl[i])
					}
				}
			}
		}
	}
}

// TestPipelinedRequests 验证同一连接上连续多条命令能被逐条正确切分
// （RPC 是一条命令一次往返，但读侧必须不粘连）。
func TestPipelinedRequests(t *testing.T) {
	for _, name := range codecNames() {
		var buf bytes.Buffer
		c := newCodec(name, nil, &buf)
		want := [][][]byte{
			{[]byte("SET"), []byte("a"), []byte("1")},
			{[]byte("SCAN"), {}, {}, []byte("10")},
			{[]byte("PING")},
		}
		for _, args := range want {
			if err := c.WriteRequest(&buf, args); err != nil {
				t.Fatal(err)
			}
		}
		r := newCodec(name, &buf, io.Discard)
		for i, args := range want {
			got, err := r.ReadRequest(&buf)
			if err != nil {
				t.Fatalf("第 %d 条: %v", i, err)
			}
			if len(got) != len(args) {
				t.Fatalf("第 %d 条参数个数 = %d, want %d", i, len(got), len(args))
			}
			for j := range got {
				if !bytes.Equal(got[j], args[j]) {
					t.Fatalf("第 %d 条参数[%d] = %q, want %q", i, j, got[j], args[j])
				}
			}
		}
	}
}

// TestTruncatedStreamIsShortRead: 报文读一半就断开，必须是 ErrShortRead
// （可判定为断连），而不是静默返回半个值。
func TestTruncatedStreamIsShortRead(t *testing.T) {
	for _, name := range codecNames() {
		var buf bytes.Buffer
		c := newCodec(name, nil, &buf)
		if err := c.WriteRequest(&buf, [][]byte{[]byte("SET"), []byte("k"), []byte("value")}); err != nil {
			t.Fatal(err)
		}
		full := buf.Bytes()
		for _, cut := range []int{1, len(full) / 2, len(full) - 1} {
			truncated := bytes.NewReader(full[:cut])
			_, err := newCodec(name, truncated, io.Discard).ReadRequest(truncated)
			if err == nil {
				t.Fatalf("%s: 截断到 %d 字节应当报错", name, cut)
			}
			if !errors.Is(err, codec.ErrShortRead) {
				t.Fatalf("%s: 截断应 ErrShortRead, got %v", name, err)
			}
		}
	}
}

// TestOversizedLengthRejected: 声明超长但实际没那么多数据时，
// 绝不能按声明值分配内存（拒绝，而不是尝试读完）。
func TestOversizedLengthRejected(t *testing.T) {
	// RESP: 声明一个超过 MaxBlobBytes 的块长度
	bad := []byte("$" + strings.Repeat("9", 12) + "\r\n")
	if _, err := codec.NewRESP(bytes.NewReader(bad), io.Discard).ReadRequest(bytes.NewReader(bad)); err == nil {
		t.Fatal("超长块长度应被拒绝")
	} else if !errors.Is(err, codec.ErrTooLarge) {
		t.Fatalf("应 ErrTooLarge, got %v", err)
	}
}

// TestEmptyRequestLineRejected: 空行必须被**拒绝**，绝不能 panic。
//
// 回归：resp codec 的 ReadRequest 曾直接取 line[0] 而不判空，于是任何客户端
// 只要发一个换行（例如 `echo >/dev/tcp/host/port` 探端口）就会 panic。
// 该 panic 发生在服务端的连接 goroutine 里，会直接打死整个进程——
// 一个换行即可远程 DoS。三个 codec 都必须以错误收场。
func TestEmptyRequestLineRejected(t *testing.T) {
	for _, name := range codecNames() {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("空行导致 panic: %v", r)
				}
			}()
			src := strings.NewReader("\n")
			_, err := newCodec(name, src, io.Discard).ReadRequest(src)
			if err == nil {
				t.Fatal("空行应报错")
			}
		})
	}
}

// TestRESPResponseShapeIsSelfChecking: 状态行必须自洽（数字与名字同类），
// 否则视为协议错误——两端实现分歧应当立刻暴露。
func TestRESPResponseShapeIsSelfChecking(t *testing.T) {
	for _, bad := range []string{
		":1\r\n+error\r\n\r\n",          // 名字与数字不符
		":999\r\n+ok\r\n\r\n",           // 越界状态
		":1\r\n\r\n",                    // 缺名字行
		"junk\r\n",                      // 不是状态行
		":1\r\n+ok\r\n7\r\nabc\r\n\r\n", // 块长度声明 7 但只有 3 字节 -> 短读
	} {
		_, _, err := codec.NewRESP(strings.NewReader(bad), io.Discard).ReadResponse(strings.NewReader(bad))
		if err == nil {
			t.Fatalf("畸形应答 %q 应当报错", bad)
		}
	}
}

// TestBinaryUvarintBoundaries: 长度字段跨越 uvarint 编码边界时仍正确。
func TestBinaryUvarintBoundaries(t *testing.T) {
	for _, n := range []int{0, 1, 127, 128, 16383, 16384, 65535} {
		val := bytes.Repeat([]byte("z"), n)
		var buf bytes.Buffer
		c := codec.NewBinary(nil, &buf)
		if err := c.WriteRequest(&buf, [][]byte{[]byte("SET"), []byte("k"), val}); err != nil {
			t.Fatal(err)
		}
		got, err := codec.NewBinary(&buf, io.Discard).ReadRequest(&buf)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if len(got) != 3 || !bytes.Equal(got[2], val) {
			t.Fatalf("n=%d 往返不一致（got len=%d）", n, len(got[2]))
		}
	}
}

// TestStatusStringStable: 状态名参与 RESP 应答的可读行，改名会破坏兼容，
// 因此这里把它钉住。
func TestStatusStringStable(t *testing.T) {
	want := map[codec.Status]string{
		codec.StatusOK:           "ok",
		codec.StatusEmpty:        "empty",
		codec.StatusError:        "error",
		codec.StatusUnsupported:  "unsupported",
		codec.StatusClosed:       "closed",
		codec.StatusNotInteger:   "not_integer",
		codec.StatusInvalidTTL:   "invalid_ttl",
		codec.StatusNotFound:     "not_found",
		codec.StatusAuthRequired: "auth_required",
		codec.StatusAuthFailed:   "auth_failed",
	}
	for st, name := range want {
		if got := st.String(); got != name {
			t.Fatalf("Status(%d).String() = %q, want %q", st, got, name)
		}
	}
}
