package kvdb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

// Marshal / Unmarshal 是**非标量**类型（结构体、切片、映射、指针等）的
// 编解码实现，默认 JSON。它们是包级变量，可在 init 中整体替换：
//
//	func init() {
//	    kvdb.Marshal = msgpack.Marshal
//	    kvdb.Unmarshal = msgpack.Unmarshal
//	}
//
// 替换后 Enc/Dec/D/DMust 对结构体等类型即使用新编解码；标量（整数/浮点/字符串/
// 布尔/[]byte）不受影响——它们仍走文本编码，以保持与 Incr 等命令的互操作。
// 需要自行处理编码错误时，直接调用 Marshal / Unmarshal。
var (
	Marshal   = func(v any) ([]byte, error) { return json.Marshal(v) }
	Unmarshal = func(data []byte, out any) error { return json.Unmarshal(data, out) }
)

var byteSliceType = reflect.TypeOf([]byte(nil))

// Enc 将 T 编码为二进制安全字节，供写路径内联使用：
//
//	db.Set(ctx, "n", Enc(int64(42)))          // 标量：十进制文本
//	db.Set(ctx, "u", Enc(User{ID: 7}))        // 结构体：默认 JSON（可换编解码）
//	db.QPush(ctx, "q", Enc([]string{"a", "b"}))
//
// 编码规则：
//   - 标量走文本编码，与 Incr 语义对齐（写入整数后仍可 Incr）：
//     整数 → 十进制；float → strconv 最短表示（'g', -1）；bool → "true"/"false"；
//     string → 原样；[]byte → 恒等
//   - 其他类型（结构体、切片、映射、指针、接口等）→ Marshal（默认 JSON）
//
// 注意：非标量编码失败会 panic（Enc 不返回错误）。需要错误处理时直接调用
// Marshal，或先自行校验。
func Enc[T any](v T) []byte {
	if b, ok := encodeScalar(v); ok {
		return b
	}
	b, err := Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("kvdb: Enc: %v", err))
	}
	return b
}

// Dec 将字节解码为 T（Enc 的逆操作）：
//   - 标量按 T 的位宽严格解析文本，越界或格式非法返回 *strconv.NumError 类错误；
//   - 其他类型走 Unmarshal（默认 JSON）。
func Dec[T any](b []byte) (T, error) {
	var out T
	rv := reflect.ValueOf(&out).Elem()
	if err := decodeScalar(rv, b); err == nil {
		return out, nil
	} else if err != errNotScalar {
		var zero T
		return zero, err
	}
	if err := Unmarshal(b, &out); err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}

// D 合并 Get/QPop 族的 (val, ok, err) 三返回值并解码为 T，
// Go 多返回值可直接作为实参展开，形态：
//
//	v, ok, err := D[int64](db.Get(ctx, "n"))   // ok 原值透传
//	u, ok, err := D[User](db.Get(ctx, "u"))    // 结构体（默认 JSON）
//	v, ok, err := D[string](db.QPop(ctx, "q")) // 空队列同样适用
//
// **ok 与 err 都原值透传，不做任何转换**：key/成员不存在时 ok=false 而 err=nil。
// 需要"缺失即错误"的语义时自行判断：
//
//	v, ok, err := D[User](db.Get(ctx, "u"))
//	if err != nil { return err }   // IO/解析错误
//	if !ok { return ErrNotFound }  // 缺失
func D[T any](val []byte, ok bool, err error) (T, bool, error) {
	var z T
	if err != nil {
		return z, false, err
	}
	if !ok {
		return z, false, nil
	}
	v, err := Dec[T](val)
	if err != nil {
		return z, false, err
	}
	return v, true, nil
}

// DMust 是 D 的 panic 变体，但**只看 err、忽略 ok**：
//
//	n := DMust[int64](db.Get(ctx, "n"))
//	u := DMust[User](db.Get(ctx, "u"))
//
// 语义边界（重要）：err != nil 时 panic(err)；**ok=false 不 panic，返回零值**。
// 也就是说它断言的是"这次读取没有出错"，而不是"值一定存在"——缺失的 key 会得到
// 零值而非 panic。需要"缺失也算失败"就用 D 自行判断。
func DMust[T any](val []byte, ok bool, err error) T {
	v, _, err := D[T](val, ok, err)
	if err != nil {
		panic(err)
	}
	return v
}

// errNotScalar 表示目标类型不是标量，需交给 Unmarshal 处理。
var errNotScalar = fmt.Errorf("kvdb: not a scalar")

// encodeScalar 按标量规则编码；返回 false 表示类型不是标量，应交给 Marshal。
func encodeScalar(v any) ([]byte, bool) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, false // nil 接口交给 Marshal
	}
	switch rv.Kind() {
	case reflect.String:
		return []byte(rv.String()), true
	case reflect.Bool:
		if rv.Bool() {
			return []byte("true"), true
		}
		return []byte("false"), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(nil, rv.Int(), 10), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.AppendUint(nil, rv.Uint(), 10), true
	case reflect.Float32:
		return strconv.AppendFloat(nil, rv.Float(), 'g', -1, 32), true
	case reflect.Float64:
		return strconv.AppendFloat(nil, rv.Float(), 'g', -1, 64), true
	case reflect.Slice:
		// 仅 []byte 恒等；其他切片（含 []MyByte 这类命名元素）走 Marshal，
		// 与 encoding/json 的 base64 约定保持一致。
		if rv.Type() == byteSliceType {
			return rv.Bytes(), true
		}
		return nil, false
	default:
		return nil, false
	}
}

// decodeScalar 把文本解码进标量目标；返回 errNotScalar 表示目标不是标量。
func decodeScalar(rv reflect.Value, b []byte) error {
	s := string(b)
	switch rv.Kind() {
	case reflect.String:
		rv.SetString(s)
		return nil
	case reflect.Bool:
		v, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		rv.SetBool(v)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err := strconv.ParseInt(s, 10, rv.Type().Bits())
		if err != nil {
			return err
		}
		rv.SetInt(v)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		v, err := strconv.ParseUint(s, 10, rv.Type().Bits())
		if err != nil {
			return err
		}
		rv.SetUint(v)
		return nil
	case reflect.Float32:
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return err
		}
		rv.SetFloat(v)
		return nil
	case reflect.Float64:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		rv.SetFloat(v)
		return nil
	case reflect.Slice:
		if rv.Type() == byteSliceType {
			rv.SetBytes(b)
			return nil
		}
		return errNotScalar
	default:
		return errNotScalar
	}
}
