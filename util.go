package store

import (
	"context"
	"reflect"
	"strings"
)

type Transaction interface{
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

func ToDelimited(s string, delimiter uint8) string {
	s = strings.TrimSpace(s)
	n := strings.Builder{}
	n.Grow(len(s) + 2) // nominal 2 bytes of extra space for inserted delimiters
	for i, v := range []byte(s) {
		vIsCap := v >= 'A' && v <= 'Z'
		vIsLow := v >= 'a' && v <= 'z'
		if vIsCap {
			v += 'a'
			v -= 'A'
		}

		// treat acronyms as words, eg for JSONData -> JSON is a whole word
		if i+1 < len(s) {
			next := s[i+1]
			vIsNum := v >= '0' && v <= '9'
			nextIsCap := next >= 'A' && next <= 'Z'
			nextIsLow := next >= 'a' && next <= 'z'
			nextIsNum := next >= '0' && next <= '9'
			// add underscore if next letter case type is changed
			if (vIsCap && (nextIsLow || nextIsNum)) || (vIsLow && (nextIsCap || nextIsNum)) || (vIsNum && (nextIsCap || nextIsLow)) {
				if vIsCap && nextIsLow {
					if prevIsCap := i > 0 && s[i-1] >= 'A' && s[i-1] <= 'Z'; prevIsCap {
						n.WriteByte(delimiter)
					}
				}
				n.WriteByte(v)
				if vIsLow || vIsNum || nextIsNum {
					n.WriteByte(delimiter)
				}
				continue
			}
		}

		if v == ' ' || v == '_' || v == '-' {
			// replace space/underscore/hyphen with delimiter
			n.WriteByte(delimiter)
		} else {
			n.WriteByte(v)
		}
	}

	return n.String()
}

func GetColumnsFromModelType(model reflect.Type, tag string) []string {
	var columns []string
	for i := 0; i < model.NumField(); i++ {
		field := model.Field(i)
		col := ""
		if val, ok := field.Tag.Lookup(tag); ok {
			col = strings.TrimSpace(strings.Split(val, ",")[0])
		}

		if col != "" {
			columns = append(columns, col)
		}
	}

	return columns
}

func ParseDBTag(value string) (name string, datatype string, option string) {
	valArr := strings.Split(value, ",")
	name = strings.TrimSpace(valArr[0])
	datatype = ""
	option = ""

	if len(valArr) > 1 {
		datatype = valArr[1]
	}

	if len(valArr) > 2 {
		option = valArr[2]
	}

	return
}
