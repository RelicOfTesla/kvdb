package main

import (
	"fmt"
	"strings"
)

// splitArgs 把一行输入切成参数，规则贴近 shell：
//
//   - 空白分隔参数（连续空白视为一个分隔符）；
//   - 单引号内原样保留（不解释转义），双引号内解释转义；
//   - 引号外的反斜杠同样解释转义（于是空参数可以写成 ""）；
//   - 支持 \n \t \r \\ \" \' \0 以及 \xNN（两位十六进制，用于传二进制值）；
//   - 未闭合引号、行尾孤立的反斜杠、非法 \x 都会报错，而不是把余下内容吞掉。
//
// 之所以自己实现而不引 shell：CLI 要能传含空格的 key、以及含换行/非 UTF-8 的
// 二进制值，而"按空白切分"会把它们切坏。
func splitArgs(line string) ([]string, error) {
	var (
		out    []string
		cur    strings.Builder
		inWord bool
		quote  rune // 0=不在引号内，否则是 ' 或 "
	)
	rs := []rune(line)
	flush := func() {
		if inWord {
			out = append(out, cur.String())
			cur.Reset()
			inWord = false
		}
	}
	for i := 0; i < len(rs); i++ {
		r := rs[i]
		switch {
		case r == '\\' && quote != '\'':
			// 单引号内不解释转义（与 shell 一致）
			if i+1 >= len(rs) {
				return nil, fmt.Errorf("行尾的 \\ 不完整")
			}
			i++
			next := rs[i]
			if next == 'x' {
				if i+2 >= len(rs) {
					return nil, fmt.Errorf("\\x 后需要两位十六进制")
				}
				hi, ok1 := hexVal(rs[i+1])
				lo, ok2 := hexVal(rs[i+2])
				if !ok1 || !ok2 {
					return nil, fmt.Errorf("\\x 后需要两位十六进制，得到 %q", string(rs[i+1:i+3]))
				}
				cur.WriteByte(hi<<4 | lo)
				i += 2
			} else {
				b, err := unescape(next)
				if err != nil {
					return nil, err
				}
				cur.WriteByte(b)
			}
			inWord = true
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
			inWord = true
		case r == '\'' || r == '"':
			quote = r
			inWord = true // 允许写出空参数 "" 或 ''
		case r == ' ' || r == '\t':
			flush()
		default:
			cur.WriteRune(r)
			inWord = true
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("引号 %c 未闭合", quote)
	}
	flush()
	return out, nil
}

func hexVal(r rune) (byte, bool) {
	switch {
	case r >= '0' && r <= '9':
		return byte(r - '0'), true
	case r >= 'a' && r <= 'f':
		return byte(r-'a') + 10, true
	case r >= 'A' && r <= 'F':
		return byte(r-'A') + 10, true
	}
	return 0, false
}

// unescape 解释一个单字符转义。
func unescape(r rune) (byte, error) {
	switch r {
	case 'n':
		return '\n', nil
	case 't':
		return '\t', nil
	case 'r':
		return '\r', nil
	case '\\':
		return '\\', nil
	case '"':
		return '"', nil
	case '\'':
		return '\'', nil
	case '0':
		return 0, nil
	}
	return 0, fmt.Errorf("不支持的转义 \\%c（支持 \\n \\t \\r \\\\ \\\" \\' \\0 \\xNN）", r)
}
