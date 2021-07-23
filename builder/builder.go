package builder

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

var (
	errSplitEmptyKey = errors.New("[builder] couldn't split a empty string")
	// ErrUnsupportedOperator reports there's unsupported operators in where-condition
	ErrUnsupportedOperator     = errors.New("[builder] unsupported operator")
	errOrValueType             = errors.New(`[builder] the value of "_or" must be of slice of map[string]interface{} type`)
	errOrderByValueType        = errors.New(`[builder] the value of "_orderby" must be of string type`)
	errGroupByValueType        = errors.New(`[builder] the value of "_groupby" must be of string type`)
	errLimitValueType          = errors.New(`[builder] the value of "_limit" must be of []uint type`)
	errLimitValueLength        = errors.New(`[builder] the value of "_limit" must contain one or two uint elements`)
	errSLimitValueType         = errors.New(`[builder] the value of "_slimit" must be of []uint type`)
	errSLimitValueLength       = errors.New(`[builder] the value of "_slimit" must contain one or two uint elements`)
	errIntervalValueType       = errors.New(`[builder] the value of "_interval" must be of *Interval type`)
	errIntervalValueError      = errors.New(`[builder] the value of "_interval" error`)
	errNeedFillType            = errors.New(`[builder] the value of "_fill" must be of FillType`)
	errFillValue               = errors.New(`[builder] the value of "_fill" must be in "NONE" "VALUE" "PREV" "NULL" "LINEAR" "NEXT"`)
	errFillMissValue           = errors.New(`[builder] the value of "_fill" is "VALUE" miss "_fillvalue"`)
	errFillValueTypeError      = errors.New(`[builder] the value of "_fillvalue" must be of Float64 type`)
	errWhereInterfaceSliceType = `[builder] the value of "xxx %s" must be of []interface{} type`
	errEmptySliceCondition     = `[builder] the value of "%s" must contain at least one element`

	defaultIgnoreKeys = map[string]struct{}{
		"_orderby":   {},
		"_groupby":   {},
		"_limit":     {},
		"_slimit":    {},
		"_interval":  {},
		"_fill":      {},
		"_fillvalue": {},
	}
)

type whereMapSet struct {
	set map[string]map[string]interface{}
}

func (w *whereMapSet) add(op, field string, val interface{}) {
	if nil == w.set {
		w.set = make(map[string]map[string]interface{})
	}
	s, ok := w.set[op]
	if !ok {
		s = make(map[string]interface{})
		w.set[op] = s
	}
	s[field] = val
}

type eleLimit struct {
	begin, step uint
}

// BuildSelect work as its name says.
// supported operators including: =,in,>,>=,<,<=,<>,!=.
// key without operator will be regarded as =.
// special key begin with _: _orderby,_groupby,_limit,_having.
// the value of _limit must be a slice whose type should be []uint and must contain two uints(ie: []uint{0, 100}).
// the value of _having must be a map just like where but only support =,in,>,>=,<,<=,<>,!=
// for more examples,see README.md or open a issue.
func BuildSelect(table string, where map[string]interface{}, selectField []string) (cond string, vals []interface{}, err error) {
	var orderBy string
	var limit *eleLimit
	var sLimit *eleLimit
	var groupBy string
	var interval string
	var fill string
	if val, ok := where["_orderby"]; ok {
		s, ok := val.(string)
		if !ok {
			err = errOrderByValueType
			return
		}
		orderBy = strings.TrimSpace(s)
	}
	if val, ok := where["_groupby"]; ok {
		s, ok := val.(string)
		if !ok {
			err = errGroupByValueType
			return
		}
		groupBy = strings.TrimSpace(s)
	}
	if val, ok := where["_limit"]; ok {
		arr, ok := val.([]uint)
		if !ok {
			err = errLimitValueType
			return
		}
		if len(arr) != 2 {
			if len(arr) == 1 {
				arr = []uint{0, arr[0]}
			} else {
				err = errLimitValueLength
				return
			}
		}
		begin, step := arr[0], arr[1]
		limit = &eleLimit{
			begin: begin,
			step:  step,
		}
	}
	if val, ok := where["_slimit"]; ok {
		arr, ok := val.([]uint)
		if !ok {
			err = errSLimitValueType
			return
		}
		if len(arr) != 2 {
			if len(arr) == 1 {
				arr = []uint{0, arr[0]}
			} else {
				err = errSLimitValueLength
				return
			}
		}
		begin, step := arr[0], arr[1]
		sLimit = &eleLimit{
			begin: begin,
			step:  step,
		}
	}
	if val, ok := where["_interval"]; ok {
		value, ok := val.(*Interval)
		if !ok {
			err = errIntervalValueType
			return
		}
		if value == nil || value.Value == 0 || value.Unit == "" {
			err = errIntervalValueError
			return
		}
		interval = value.String()
	}
	if val, ok := where["_fill"]; ok {
		value, ok := val.(FillType)
		if !ok {
			err = errNeedFillType
			return
		}
		if _, exist := FillTypeMap[value]; !exist {
			err = errFillValue
			return
		}
		if value == FillValue {
			fillValue, ok := where["_fillvalue"]
			if !ok {
				err = errFillMissValue
				return
			}
			v, ok := fillValue.(float64)
			if !ok {
				err = errFillValueTypeError
				return
			}
			fill = fmt.Sprintf("(VALUE,%f)", v)
		} else {
			fill = string(value)
		}
	}
	conditions, err := getWhereConditions(where, defaultIgnoreKeys)
	if err != nil {
		return
	}
	return buildSelect(table, selectField, groupBy, orderBy, sLimit, limit, interval, fill, conditions...)
}

// BuildInsert work as its name says
func BuildInsert(table string, data [][]interface{}) (string, []interface{}, error) {
	return buildInsert(table, "", nil, data, commonInsert)
}

func BuildInsertStable(table string, sTable string, tags []interface{}, data [][]interface{}) (string, []interface{}, error) {
	return buildInsert(table, sTable, tags, data, commonInsert)
}

func isStringInSlice(str string, arr []string) bool {
	for _, s := range arr {
		if s == str {
			return true
		}
	}
	return false
}

func getWhereConditions(where map[string]interface{}, ignoreKeys map[string]struct{}) ([]Comparable, error) {
	if len(where) == 0 {
		return nil, nil
	}
	wms := &whereMapSet{}
	var comparables []Comparable
	var field, operator string
	var err error
	for key, val := range where {
		if _, ok := ignoreKeys[key]; ok {
			continue
		}
		if key == "_or" {
			var (
				orWheres          []map[string]interface{}
				orWhereComparable []Comparable
				ok                bool
			)
			if orWheres, ok = val.([]map[string]interface{}); !ok {
				return nil, errOrValueType
			}
			for _, orWhere := range orWheres {
				if orWhere == nil {
					continue
				}
				orNestWhere, err := getWhereConditions(orWhere, ignoreKeys)
				if err != nil {
					return nil, err
				}
				orWhereComparable = append(orWhereComparable, NestWhere(orNestWhere))
			}
			comparables = append(comparables, OrWhere(orWhereComparable))
			continue
		}
		field, operator, err = splitKey(key, val)
		if err != nil {
			return nil, err
		}
		operator = strings.ToLower(operator)
		if !isStringInSlice(operator, opOrder) {
			return nil, ErrUnsupportedOperator
		}
		if _, ok := val.(NullType); ok {
			operator = opNull
		}
		wms.add(operator, field, val)
	}
	whereComparables, err := buildWhereCondition(wms)
	if err != nil {
		return nil, err
	}
	comparables = append(comparables, whereComparables...)
	return comparables, nil
}

const (
	opEq         = "="
	opNe1        = "!="
	opNe2        = "<>"
	opIn         = "in"
	opNotIn      = "not in"
	opGt         = ">"
	opGte        = ">="
	opLt         = "<"
	opLte        = "<="
	opLike       = "like"
	opNotLike    = "not like"
	opBetween    = "between"
	opNotBetween = "not between"
	// special
	opNull = "null"
)

type compareProducer func(m map[string]interface{}) (Comparable, error)

var op2Comparable = map[string]compareProducer{
	opEq: func(m map[string]interface{}) (Comparable, error) {
		return Eq(m), nil
	},
	opNe1: func(m map[string]interface{}) (Comparable, error) {
		return Ne(m), nil
	},
	opNe2: func(m map[string]interface{}) (Comparable, error) {
		return Ne(m), nil
	},
	opIn: func(m map[string]interface{}) (Comparable, error) {
		wp, err := convertWhereMapToWhereMapSlice(m, opIn)
		if err != nil {
			return nil, err
		}
		return In(wp), nil
	},
	opNotIn: func(m map[string]interface{}) (Comparable, error) {
		wp, err := convertWhereMapToWhereMapSlice(m, opNotIn)
		if err != nil {
			return nil, err
		}
		return NotIn(wp), nil
	},
	opBetween: func(m map[string]interface{}) (Comparable, error) {
		wp, err := convertWhereMapToWhereMapSlice(m, opBetween)
		if err != nil {
			return nil, err
		}
		return Between(wp), nil
	},
	opNotBetween: func(m map[string]interface{}) (Comparable, error) {
		wp, err := convertWhereMapToWhereMapSlice(m, opNotBetween)
		if err != nil {
			return nil, err
		}
		return NotBetween(wp), nil
	},
	opGt: func(m map[string]interface{}) (Comparable, error) {
		return Gt(m), nil
	},
	opGte: func(m map[string]interface{}) (Comparable, error) {
		return Gte(m), nil
	},
	opLt: func(m map[string]interface{}) (Comparable, error) {
		return Lt(m), nil
	},
	opLte: func(m map[string]interface{}) (Comparable, error) {
		return Lte(m), nil
	},
	opLike: func(m map[string]interface{}) (Comparable, error) {
		return Like(m), nil
	},
	opNotLike: func(m map[string]interface{}) (Comparable, error) {
		return NotLike(m), nil
	},
	opNull: func(m map[string]interface{}) (Comparable, error) {
		return nullCompareble(m), nil
	},
}

var opOrder = []string{opEq, opIn, opNe1, opNe2, opNotIn, opGt, opGte, opLt, opLte, opLike, opNotLike, opBetween, opNotBetween, opNull}

func buildWhereCondition(mapSet *whereMapSet) ([]Comparable, error) {
	var cpArr []Comparable
	for _, operator := range opOrder {
		whereMap, ok := mapSet.set[operator]
		if !ok {
			continue
		}
		f, ok := op2Comparable[operator]
		if !ok {
			return nil, ErrUnsupportedOperator
		}
		cp, err := f(whereMap)
		if err != nil {
			return nil, err
		}
		cpArr = append(cpArr, cp)
	}
	return cpArr, nil
}

func convertWhereMapToWhereMapSlice(where map[string]interface{}, op string) (map[string][]interface{}, error) {
	result := make(map[string][]interface{})
	for key, val := range where {
		vals, ok := convertInterfaceToMap(val)
		if !ok {
			return nil, fmt.Errorf(errWhereInterfaceSliceType, op)
		}
		if 0 == len(vals) {
			return nil, fmt.Errorf(errEmptySliceCondition, op)
		}
		result[key] = vals
	}
	return result, nil
}

func convertInterfaceToMap(val interface{}) ([]interface{}, bool) {
	s := reflect.ValueOf(val)
	if s.Kind() != reflect.Slice {
		return nil, false
	}
	interfaceSlice := make([]interface{}, s.Len())
	for i := 0; i < s.Len(); i++ {
		interfaceSlice[i] = s.Index(i).Interface()
	}
	return interfaceSlice, true
}

func splitKey(key string, val interface{}) (field string, operator string, err error) {
	key = strings.Trim(key, " ")
	if "" == key {
		err = errSplitEmptyKey
		return
	}
	idx := strings.IndexByte(key, ' ')
	if idx == -1 {
		field = key
		operator = "="
		if reflect.ValueOf(val).Kind() == reflect.Slice {
			operator = "in"
		}
	} else {
		field = key[:idx]
		operator = strings.Trim(key[idx+1:], " ")
		operator = removeInnerSpace(operator)
	}
	return
}

func removeInnerSpace(operator string) string {
	n := len(operator)
	firstSpace := strings.IndexByte(operator, ' ')
	if firstSpace == -1 {
		return operator
	}
	lastSpace := firstSpace
	for i := firstSpace + 1; i < n; i++ {
		if operator[i] == ' ' {
			lastSpace = i
		} else {
			break
		}
	}
	return operator[:firstSpace] + operator[lastSpace:]
}

const (
	paramPlaceHolder = "?"
)

var searchHandle = regexp.MustCompile(`{{\S+?}}`)

// NamedQuery is used for expressing complex query
func NamedQuery(sql string, data map[string]interface{}) (string, []interface{}, error) {
	length := len(data)
	if length == 0 {
		return sql, nil, nil
	}
	vals := make([]interface{}, 0, length)
	var err error
	cond := searchHandle.ReplaceAllStringFunc(sql, func(paramName string) string {
		paramName = strings.TrimRight(strings.TrimLeft(paramName, "{"), "}")
		val, ok := data[paramName]
		if !ok {
			err = fmt.Errorf("%s not found", paramName)
			return ""
		}
		v := reflect.ValueOf(val)
		if v.Type().Kind() != reflect.Slice {
			vals = append(vals, val)
			return paramPlaceHolder
		}
		length := v.Len()
		for i := 0; i < length; i++ {
			vals = append(vals, v.Index(i).Interface())
		}
		return createMultiPlaceholders(length)
	})
	if err != nil {
		return "", nil, err
	}
	return cond, vals, nil
}

func createMultiPlaceholders(num int) string {
	if 0 == num {
		return ""
	}
	length := (num << 1) | 1
	buff := make([]byte, length)
	buff[0], buff[length-1] = '(', ')'
	ll := length - 2
	for i := 1; i <= ll; i += 2 {
		buff[i] = '?'
	}
	ll = length - 3
	for i := 2; i <= ll; i += 2 {
		buff[i] = ','
	}
	return string(buff)
}

type IntervalUnitType string

//u(微秒)、a(毫秒)、s(秒)、m(分)、h(小时)、d(天)、w(周) n(自然月) 和 y(自然年)
const (
	Microsecond IntervalUnitType = "u"
	Millisecond IntervalUnitType = "a"
	Second      IntervalUnitType = "s"
	Minute      IntervalUnitType = "m"
	Hour        IntervalUnitType = "h"
	Day         IntervalUnitType = "d"
	Week        IntervalUnitType = "w"
	Month       IntervalUnitType = "n"
	Year        IntervalUnitType = "y"
)

type Interval struct {
	Value uint
	Unit  IntervalUnitType
}

func (i *Interval) String() string {
	if i == nil {
		return ""
	}
	return fmt.Sprintf("%d%s", i.Value, i.Unit)
}

type FillType string

var FillTypeMap = map[FillType]struct{}{
	FillNone:   {},
	FillValue:  {},
	FillPrev:   {},
	FillNull:   {},
	FillLinear: {},
	FillNext:   {},
}

const (
	FillNone   FillType = "NONE"
	FillValue  FillType = "VALUE"
	FillPrev   FillType = "PREV"
	FillNull   FillType = "NULL"
	FillLinear FillType = "LINEAR"
	FillNext   FillType = "NEXT"
)
