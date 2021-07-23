package builder

import (
	"errors"
	"fmt"
	"github.com/didi/gendry/builder/pool"
	"sort"
	"strings"
)

var (
	errInsertNullData = errors.New("insert null data")
)

//the order of a map is unpredicatable so we need a sort algorithm to sort the fields
//and make it predicatable
var (
	defaultSortAlgorithm = sort.Strings
)

//Comparable requires type implements the Build method
type Comparable interface {
	Build() ([]string, []interface{})
}

// NullType is the NULL type in mysql
type NullType byte

func (nt NullType) String() string {
	if nt == IsNull {
		return "is null"
	}
	return "is not null"
}

const (
	_ NullType = iota
	// IsNull the same as `is null`
	IsNull
	// IsNotNull the same as `is not null`
	IsNotNull
)

type nullCompareble map[string]interface{}

func (n nullCompareble) Build() ([]string, []interface{}) {
	length := len(n)
	if nil == n || 0 == length {
		return nil, nil
	}
	sortedKey := make([]string, 0, length)
	cond := make([]string, 0, length)
	for k := range n {
		sortedKey = append(sortedKey, k)
	}
	defaultSortAlgorithm(sortedKey)
	for _, field := range sortedKey {
		v, ok := n[field]
		if !ok {
			continue
		}
		rv, ok := v.(NullType)
		if !ok {
			continue
		}
		cond = append(cond, field+" "+rv.String())
	}
	return cond, nil
}

type nilComparable byte

func (n nilComparable) Build() ([]string, []interface{}) {
	return nil, nil
}

// Like means like
type Like map[string]interface{}

// Build implements the Comparable interface
func (l Like) Build() ([]string, []interface{}) {
	if nil == l || 0 == len(l) {
		return nil, nil
	}
	var cond []string
	var vals []interface{}
	for k := range l {
		cond = append(cond, k)
	}
	defaultSortAlgorithm(cond)
	for j := 0; j < len(cond); j++ {
		val := l[cond[j]]
		cond[j] = cond[j] + " like ?"
		vals = append(vals, val)
	}
	return cond, vals
}

type NotLike map[string]interface{}

// Build implements the Comparable interface
func (l NotLike) Build() ([]string, []interface{}) {
	if nil == l || 0 == len(l) {
		return nil, nil
	}
	var cond []string
	var vals []interface{}
	for k := range l {
		cond = append(cond, k)
	}
	defaultSortAlgorithm(cond)
	for j := 0; j < len(cond); j++ {
		val := l[cond[j]]
		cond[j] = cond[j] + " not like ?"
		vals = append(vals, val)
	}
	return cond, vals
}

//Eq means equal(=)
type Eq map[string]interface{}

//Build implements the Comparable interface
func (e Eq) Build() ([]string, []interface{}) {
	return build(e, "=")
}

//Ne means Not Equal(!=)
type Ne map[string]interface{}

//Build implements the Comparable interface
func (n Ne) Build() ([]string, []interface{}) {
	return build(n, "!=")
}

//Lt means less than(<)
type Lt map[string]interface{}

//Build implements the Comparable interface
func (l Lt) Build() ([]string, []interface{}) {
	return build(l, "<")
}

//Lte means less than or equal(<=)
type Lte map[string]interface{}

//Build implements the Comparable interface
func (l Lte) Build() ([]string, []interface{}) {
	return build(l, "<=")
}

//Gt means greater than(>)
type Gt map[string]interface{}

//Build implements the Comparable interface
func (g Gt) Build() ([]string, []interface{}) {
	return build(g, ">")
}

//Gte means greater than or equal(>=)
type Gte map[string]interface{}

//Build implements the Comparable interface
func (g Gte) Build() ([]string, []interface{}) {
	return build(g, ">=")
}

//In means in
type In map[string][]interface{}

//Build implements the Comparable interface
func (i In) Build() ([]string, []interface{}) {
	if nil == i || 0 == len(i) {
		return nil, nil
	}
	var cond []string
	var vals []interface{}
	for k := range i {
		cond = append(cond, k)
	}
	defaultSortAlgorithm(cond)
	for j := 0; j < len(cond); j++ {
		val := i[cond[j]]
		cond[j] = buildIn(cond[j], val)
		vals = append(vals, val...)
	}
	return cond, vals
}

func buildIn(field string, vals []interface{}) (cond string) {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.Grow(len(field) + len(vals)*2 + 5)
	b.WriteString(field)
	b.WriteString(" in (")

	for i := 0; i < len(vals); i++ {
		b.WriteByte('?')
		if i != len(vals)-1 {
			b.WriteByte(',')
		}
	}
	b.WriteByte(')')
	return b.String()
}

//NotIn means not in
type NotIn map[string][]interface{}

//Build implements the Comparable interface
func (i NotIn) Build() ([]string, []interface{}) {
	if nil == i || 0 == len(i) {
		return nil, nil
	}
	var cond []string
	var vals []interface{}
	for k := range i {
		cond = append(cond, k)
	}
	defaultSortAlgorithm(cond)
	for j := 0; j < len(cond); j++ {
		val := i[cond[j]]
		cond[j] = buildNotIn(cond[j], val)
		vals = append(vals, val...)
	}
	return cond, vals
}

func buildNotIn(field string, vals []interface{}) (cond string) {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.Grow(len(field) + len(vals)*2 + 9)
	b.WriteString(field)
	b.WriteString(" not in (")

	for i := 0; i < len(vals); i++ {
		b.WriteByte('?')
		if i != len(vals)-1 {
			b.WriteByte(',')
		}
	}
	b.WriteByte(')')
	return b.String()
}

type Between map[string][]interface{}

func (bt Between) Build() ([]string, []interface{}) {
	return betweenBuilder(bt, false)
}

func betweenBuilder(bt map[string][]interface{}, notBetween bool) ([]string, []interface{}) {
	if len(bt) == 0 {
		return nil, nil
	}
	var cond []string
	var vals []interface{}
	for k := range bt {
		cond = append(cond, k)
	}
	defaultSortAlgorithm(cond)
	for j := 0; j < len(cond); j++ {
		val := bt[cond[j]]
		cond_j, err := buildBetween(notBetween, cond[j], val)
		if err != nil {
			continue
		}
		cond[j] = cond_j
		vals = append(vals, val...)
	}
	return cond, vals
}

type NotBetween map[string][]interface{}

func (nbt NotBetween) Build() ([]string, []interface{}) {
	return betweenBuilder(nbt, true)
}

func buildBetween(notBetween bool, key string, vals []interface{}) (string, error) {
	if len(vals) != 2 {
		return "", errors.New("vals of between must be a slice with two elements")
	}
	var operator string
	if notBetween {
		operator = "not between"
	} else {
		operator = "between"
	}
	return fmt.Sprintf("(%s %s ? and ?)", key, operator), nil
}

type NestWhere []Comparable

func (nw NestWhere) Build() ([]string, []interface{}) {
	var cond []string
	var vals []interface{}
	nestWhereString, nestWhereVals := whereConnector("and", nw...)
	cond = append(cond, nestWhereString)
	vals = nestWhereVals
	return cond, vals
}

type OrWhere []Comparable

func (ow OrWhere) Build() ([]string, []interface{}) {
	var cond []string
	var vals []interface{}
	orWhereString, orWhereVals := whereConnector("or", ow...)
	cond = append(cond, orWhereString)
	vals = orWhereVals
	return cond, vals
}

func build(m map[string]interface{}, op string) ([]string, []interface{}) {
	if nil == m || 0 == len(m) {
		return nil, nil
	}
	length := len(m)
	cond := make([]string, length)
	vals := make([]interface{}, length)
	var i int
	for key := range m {
		cond[i] = key
		i++
	}
	defaultSortAlgorithm(cond)
	for i = 0; i < length; i++ {
		vals[i] = m[cond[i]]
		cond[i] = assembleExpression(cond[i], op)
	}
	return cond, vals
}

func assembleExpression(field, op string) string {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.Grow(len(field) + len(op) + 1)
	b.WriteString(field)
	b.WriteString(op)
	b.WriteByte('?')
	return b.String()
}

func resolveKV(m map[string]interface{}) (keys []string, vals []interface{}) {
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vals = append(vals, m[k])
	}
	return
}

func resolveFields(m map[string]interface{}) []string {
	var fields []string
	for k := range m {
		fields = append(fields, k)
	}
	defaultSortAlgorithm(fields)
	return fields
}

func whereConnector(andOr string, conditions ...Comparable) (string, []interface{}) {
	if len(conditions) == 0 {
		return "", nil
	}
	var where []string
	var values []interface{}
	for _, cond := range conditions {
		cons, vals := cond.Build()
		if nil == cons {
			continue
		}
		where = append(where, cons...)
		values = append(values, vals...)
	}
	if 0 == len(where) {
		return "", nil
	}
	whereString := "(" + strings.Join(where, " "+andOr+" ") + ")"
	return whereString, values
}

type insertType string

const (
	commonInsert insertType = "insert into"
)

func buildInsert(table string, sTable string, tags []interface{}, setList [][]interface{}, insertType insertType) (string, []interface{}, error) {
	//	INSERT INTO
	//	tb_name
	//	[USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
	//	[(field1_name, ...)]
	//	VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
	//	[tb2_name
	//	[USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
	//	[(field1_name, ...)]
	//	VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
	//	...];
	if len(setList) < 1 {
		return "", nil, errInsertNullData
	}
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString(string(insertType))
	b.WriteByte(' ')
	b.WriteString(table)
	var vals []interface{}
	if sTable != "" {
		b.WriteString(" using ")
		b.WriteString(sTable)
		b.WriteString(" tags(")
		b.WriteString(strings.TrimRight(strings.Repeat("?,", len(tags)), ","))
		b.WriteByte(')')
		for _, tag := range tags {
			vals = append(vals, tag)
		}
	}
	b.WriteString(" values ")
	placeholder := "(" + strings.TrimRight(strings.Repeat("?,", len(setList[0])), ",") + ")"
	for i := 0; i < len(setList); i++ {
		b.WriteString(placeholder)
		if i != len(setList)-1 {
			b.WriteByte(',')
		}
		vals = append(vals, setList[i]...)
	}
	return b.String(), vals, nil
}

func buildSelect(table string, uFields []string, groupBy, orderBy string, sLimit, limit *eleLimit, interval string, fill string, conditions ...Comparable) (string, []interface{}, error) {
	fields := "*"
	if len(uFields) > 0 {
		fields = strings.Join(uFields, ",")
	}
	bd := strings.Builder{}
	bd.WriteString("select ")
	bd.WriteString(fields)
	bd.WriteString(" from ")
	bd.WriteString(table)
	whereString, vals := whereConnector("and", conditions...)
	if whereString != "" {
		bd.WriteString(" where ")
		bd.WriteString(whereString)
	}
	if interval != "" {
		bd.WriteString(" interval(")
		bd.WriteString(interval)
		bd.WriteByte(')')
	}
	if fill != "" {
		bd.WriteString(" fill ")
		bd.WriteString(fill)
	}
	if groupBy != "" {
		bd.WriteString(" group by ")
		bd.WriteString(groupBy)
	}
	if orderBy != "" {
		bd.WriteString(" order by ")
		bd.WriteString(orderBy)
	}
	if sLimit != nil {
		bd.WriteString(" slimit ?,?")
		vals = append(vals, int(sLimit.begin), int(sLimit.step))
	}
	if limit != nil {
		bd.WriteString(" limit ?,?")
		vals = append(vals, int(limit.begin), int(limit.step))
	}
	return bd.String(), vals, nil
}
