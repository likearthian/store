package store

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type Transaction interface {
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

func ParseDBTag(value string) (name string, size int, isAuto bool, isKey bool, allowNull bool) {
	tagArr := strings.Split(value, ",")
	if len(tagArr) == 0 {
		return
	}

	checkBool := func(key string, tagarr []string) bool {
		bval := false
		skey := strings.TrimSpace(tagarr[0])
		if strings.EqualFold(skey, key) {
			bval = true
		}

		if len(tagarr) > 1 {
			sval := strings.TrimSpace(tagarr[1])
			if strings.EqualFold(sval, "true") {
				bval = true
			}

			if strings.EqualFold(sval, "false") {
				bval = false
			}
		}

		return bval
	}

	name = strings.TrimSpace(tagArr[0])
	if len(tagArr) > 1 {
		det := strings.Split(tagArr[1], " ")
		for _, v := range det {
			varr := strings.Split(v, "=")
			key := strings.TrimSpace(varr[0])

			if checkBool("auto", varr) {
				isAuto = true
				continue
			}

			if checkBool("key", varr) {
				isKey = true
				allowNull = false
				continue
			}

			if checkBool("allownull", varr) {
				allowNull = true && !isKey
				continue
			}

			if len(varr) > 1 {
				if strings.EqualFold(key, "size") {
					size, _ = strconv.Atoi(varr[1])
				}
			}
		}
	}

	return
}

type contextKey string

const (
	ContextTransactionKey = "tx"
)

func MergeChan[T any](exit <-chan struct{}, cs ...<-chan T) chan T {
	var wg sync.WaitGroup
	out := make(chan T)

	// Start an output goroutine for each input channel in cs.  output
	// copies va=lues from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan T) {
		defer func() {
			//fmt.Println("wg.Done()")
			wg.Done()
		}()
		for n := range c {
			select {
			case out <- n:
			case <-exit:
				return
			}
		}
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
