package builder

import (
	"context"
	"database/sql"
	"github.com/didi/gendry/builder/pool"
	"reflect"
	"strconv"
)

// AggregateQuery is a helper function to execute the aggregate query and return the result
func AggregateQuery(ctx context.Context, db *sql.DB, table string, where map[string]interface{}, aggregate AggregateSymbleBuilder) (ResultResolver, error) {
	cond, vals, err := BuildSelect(table, where, []string{aggregate.Symble()})
	if err != nil {
		return resultResolve{0}, err
	}
	rows, err := db.QueryContext(ctx, cond, vals...)
	if err != nil {
		return resultResolve{0}, err
	}
	var result interface{}
	for rows.Next() {
		err = rows.Scan(&result)
	}
	rows.Close()
	return resultResolve{result}, err
}

// ResultResolver is a helper for retrieving data
// caller should know the type and call the responding method
type ResultResolver interface {
	Int64() int64
	Float64() float64
}

type resultResolve struct {
	data interface{}
}

func (r resultResolve) Int64() int64 {
	switch t := r.data.(type) {
	case int64:
		return t
	case int32:
		return int64(t)
	case int:
		return int64(t)
	case float64:
		return int64(t)
	case float32:
		return int64(t)
	case []uint8:
		i64, err := strconv.ParseInt(string(t), 10, 64)
		if err != nil {
			return int64(r.Float64())
		}
		return i64
	default:
		return 0
	}
}

// from go-mysql-driver/mysql the value returned could be int64 float64 float32

func (r resultResolve) Float64() float64 {
	switch t := r.data.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case []uint8:
		f64, _ := strconv.ParseFloat(string(t), 64)
		return f64
	default:
		return float64(r.Int64())
	}
}

// AggregateSymbleBuilder need to be implemented so that executor can
// get what should be put into `select Symble() from xxx where yyy`
type AggregateSymbleBuilder interface {
	Symble() string
}

type agBuilder struct {
	expr  string
	alias string
}

func (a *agBuilder) Symble() string {
	if a.expr == "" {
		return ""
	}
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString(a.expr)
	if a.alias != "" {
		b.WriteString(" as ")
		b.WriteString(a.alias)
	}
	return b.String()
}

func (a *agBuilder) AS(alias string) *agBuilder {
	a.alias = alias
	return a
}

//count, max, min, avg, sum, twa, stddev, leastsquares, top, bottom, first, last, percentile, apercentile, last_row, spread, diff

// AggregateCount count(col)
func AggregateCount(col string) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("count(")
	b.WriteString(col)
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

// AggregateMax max(col)
func AggregateMax(col string) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("max(")
	b.WriteString(col)
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

// AggregateMin min(col)
func AggregateMin(col string) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("min(")
	b.WriteString(col)
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

// AggregateAvg avg(col)
func AggregateAvg(col string) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("avg(")
	b.WriteString(col)
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

// AggregateSum sum(col)
func AggregateSum(col string) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("sum(")
	b.WriteString(col)
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

func AggregateTwa(col string) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("twa(")
	b.WriteString(col)
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

func AggregateStddev(col string) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("stddev(")
	b.WriteString(col)
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

func AggregateLeastsquares(col string, startVal, stepVal int) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("leastsquares(")
	b.WriteString(col)
	b.WriteByte(',')
	b.WriteString(strconv.Itoa(startVal))
	b.WriteByte(',')
	b.WriteString(strconv.Itoa(stepVal))
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

func AggregateTop(col string, k int) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("top(")
	b.WriteString(col)
	b.WriteByte(',')
	b.WriteString(strconv.Itoa(k))
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

func AggregateBottom(col string, k int) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("bottom(")
	b.WriteString(col)
	b.WriteByte(',')
	b.WriteString(strconv.Itoa(k))
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

func AggregateFirst(col string) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("first(")
	b.WriteString(col)
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

func AggregateLast(col string) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("last(")
	b.WriteString(col)
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

func AggregatePercentile(col string, p int) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("percentile(")
	b.WriteString(col)
	b.WriteByte(',')
	b.WriteString(strconv.Itoa(p))
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}
func AggregateAPercentile(col string, p int) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("apercentile(")
	b.WriteString(col)
	b.WriteByte(',')
	b.WriteString(strconv.Itoa(p))
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}
func AggregateLastRow(col string) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("last_row(")
	b.WriteString(col)
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}
func AggregateSpread(col string) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("spread(")
	b.WriteString(col)
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

func AggregateDiff(col string) *agBuilder {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("diff(")
	b.WriteString(col)
	b.WriteByte(')')
	return &agBuilder{expr: b.String()}
}

// OmitEmpty is a helper function to clear where map zero value
func OmitEmpty(where map[string]interface{}, omitKey []string) map[string]interface{} {
	for _, key := range omitKey {
		v, ok := where[key]
		if !ok {
			continue
		}

		if isZero(reflect.ValueOf(v)) {
			delete(where, key)
		}
	}
	return where
}

type IsZeroer interface {
	IsZero() bool
}

var IsZeroType = reflect.TypeOf((*IsZeroer)(nil)).Elem()

// isZero reports whether a value is a zero value
// Including support: Bool, Array, String, Float32, Float64, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr
// Map, Slice, Interface, Struct
func isZero(v reflect.Value) bool {
	if v.IsValid() && v.Type().Implements(IsZeroType) {
		return v.Interface().(IsZeroer).IsZero()
	}
	switch v.Kind() {
	case reflect.Bool:
		return !v.Bool()
	case reflect.Array, reflect.String:
		return v.Len() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Map, reflect.Slice:
		return v.IsNil() || v.Len() == 0
	case reflect.Interface:
		return v.IsNil()
	case reflect.Invalid:
		return true
	}

	if v.Kind() != reflect.Struct {
		return false
	}

	// Traverse the Struct and only return true
	// if all of its fields return IsZero == true
	n := v.NumField()
	for i := 0; i < n; i++ {
		vf := v.Field(i)
		if !isZero(vf) {
			return false
		}
	}
	return true
}
