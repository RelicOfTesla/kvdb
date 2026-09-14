package kvdb

import (
	"reflect"
	"strconv"

	"github.com/RelicOfTesla/kvdb/core"
)

// BytesAble 是 B/P/D/DMust 支持的标量集合：整数家族（含 ~ 底层类型别名、
// 即自定义 type MyID int64 也支持）、浮点、string、bool 与 []byte（恒等）。
type BytesAble interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 | ~string | ~bool | []byte
}

// B 将 T 编码为二进制安全字节，供写路径内联使用：
//
//	db.Set(ctx, "n", B(int64(42)))
//	db.SetEx(ctx, "s", B("token"), 3600)
//	db.QPush(ctx, "q", B(score))
//
// 编码规则（与 Incr 语义对齐，写入 int64 后仍可 Incr）：
//   - 整数 → 十进制文本（int64 可被 Incr/Get 互操作）
//   - float → strconv 最短表示（'g', -1）
//   - bool → "true"/"false"
//   - string → 原样；[]byte → 恒等
func B[T BytesAble](v T) []byte {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		return []byte(rv.String())
	case reflect.Slice: // []byte 恒等
		return rv.Bytes()
	case reflect.Bool:
		if rv.Bool() {
			return []byte("true")
		}
		return []byte("false")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(nil, rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.AppendUint(nil, rv.Uint(), 10)
	case reflect.Float32:
		return strconv.AppendFloat(nil, rv.Float(), 'g', -1, 32)
	case reflect.Float64:
		return strconv.AppendFloat(nil, rv.Float(), 'g', -1, 64)
	default:
		panic("kvdb: B: unsupported type " + rv.Kind().String())
	}
}

// P 将字节解码为 T（B 的逆操作）；数值按 T 的位宽严格解析，
// 越界或格式非法返回 *strconv.NumError 类错误。
func P[T BytesAble](b []byte) (T, error) {
	var out T
	rv := reflect.ValueOf(&out).Elem()
	s := string(b)
	switch rv.Kind() {
	case reflect.String:
		rv.SetString(s)
	case reflect.Slice: // []byte 恒等
		rv.SetBytes(b)
	case reflect.Bool:
		v, err := strconv.ParseBool(s)
		if err != nil {
			return out, err
		}
		rv.SetBool(v)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err := strconv.ParseInt(s, 10, rv.Type().Bits())
		if err != nil {
			return out, err
		}
		rv.SetInt(v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		v, err := strconv.ParseUint(s, 10, rv.Type().Bits())
		if err != nil {
			return out, err
		}
		rv.SetUint(v)
	case reflect.Float32:
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return out, err
		}
		rv.SetFloat(v)
	case reflect.Float64:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return out, err
		}
		rv.SetFloat(v)
	default:
		return out, &strconv.NumError{
			Func: "P", Num: s, Err: strconv.ErrSyntax,
		}
	}
	return out, nil
}

// D 合并 Get/QPop 族的 (val, ok, err) 三返回值并解码为 T，
// Go 多返回值可直接作为实参展开，形态：
//
//	n, err := D[int64](db.Get(ctx, "n"))     // 缺 key -> ErrNotFound
//	v, err := D[string](db.QPop(ctx, "q"))   // 空队列同样适用
//
// ok=false（key/成员不存在）转换为 core.ErrNotFound；err 原样透传。
func D[T BytesAble](val []byte, ok bool, err error) (T, error) {
	var z T
	if err != nil {
		return z, err
	}
	if !ok {
		return z, core.ErrNotFound
	}
	return P[T](val)
}

// DMust 是 D 的 panic 变体：缺失或解析失败直接 panic(err)：
//
//	n := DMust[int64](db.Get(ctx, "n"))
func DMust[T BytesAble](val []byte, ok bool, err error) T {
	v, err := D[T](val, ok, err)
	if err != nil {
		panic(err)
	}
	return v
}
