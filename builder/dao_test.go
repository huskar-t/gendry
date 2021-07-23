package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEq(t *testing.T) {
	var testData = []struct {
		in     map[string]interface{}
		outCon []string
		outVal []interface{}
	}{
		{
			map[string]interface{}{
				"foo": "bar",
				"baz": 1,
				"qq":  "ttx",
			},
			[]string{"baz=?", "foo=?", "qq=?"},
			[]interface{}{1, "bar", "ttx"},
		},
	}
	ass := assert.New(t)
	for _, testCase := range testData {
		cond, vals := Eq(testCase.in).Build()
		ass.Equal(len(cond), len(vals))
		ass.Equal(testCase.outCon, cond)
		ass.Equal(testCase.outVal, vals)
	}
}

func TestIn(t *testing.T) {
	var testData = []struct {
		in      map[string][]interface{}
		outCond []string
		outVals []interface{}
	}{
		{
			in: map[string][]interface{}{
				"foo": {"bar", "baz"},
				"age": {5, 7, 9, 11},
			},
			outCond: []string{"age in (?,?,?,?)", "foo in (?,?)"},
			outVals: []interface{}{5, 7, 9, 11, "bar", "baz"},
		},
	}
	ass := assert.New(t)
	for _, testCase := range testData {
		cond, vals := In(testCase.in).Build()
		ass.Equal(testCase.outCond, cond)
		ass.Equal(testCase.outVals, vals)
	}
}

func TestNestWhere(t *testing.T) {
	var testData = []struct {
		in      NestWhere
		outCond []string
		outVals []interface{}
	}{
		{
			in: NestWhere([]Comparable{
				Eq(map[string]interface{}{
					"aa": 3,
				}),
				Eq(map[string]interface{}{
					"bb": 4,
				}),
			}),
			outCond: []string{"(aa=? and bb=?)"},
			outVals: []interface{}{3, 4},
		},
	}
	ass := assert.New(t)
	for _, testCase := range testData {
		cond, vals := testCase.in.Build()
		ass.Equal(testCase.outCond, cond)
		ass.Equal(testCase.outVals, vals)
	}
}

func TestResolveFields(t *testing.T) {
	ass := assert.New(t)
	m := map[string]interface{}{
		"foo": 1,
		"bar": 2,
		"qq":  3,
		"asd": 4,
	}
	res := resolveFields(m)
	var assertion []string
	defaultSortAlgorithm(append(assertion, "foo", "bar", "qq", "asd"))
	for i := 0; i < len(assertion); i++ {
		ass.Equal(assertion[i], res[i])
	}
}

func TestAssembleExpression(t *testing.T) {
	var data = []struct {
		inField, inOp string
		out           string
	}{
		{"foo", "=", "foo=?"},
		{"qq", "<>", "qq<>?"},
	}
	ass := assert.New(t)
	for _, tc := range data {
		ass.Equal(tc.out, assembleExpression(tc.inField, tc.inOp))
	}
}

func TestResolveKV(t *testing.T) {
	var data = []struct {
		in      map[string]interface{}
		outStr  []string
		outVals []interface{}
	}{
		{
			map[string]interface{}{
				"foo": "bar",
				"bar": 1,
			},
			[]string{"bar", "foo"},
			[]interface{}{1, "bar"},
		},
		{
			map[string]interface{}{
				"qq":    "ttt",
				"some":  123,
				"other": 456,
			},
			[]string{"other", "qq", "some"},
			[]interface{}{456, "ttt", 123},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		keys, vals := resolveKV(tc.in)
		ass.Equal(tc.outStr, keys)
		ass.Equal(tc.outVals, vals)
	}
}

func TestWhereConnector(t *testing.T) {
	var data = []struct {
		in      []Comparable
		outStr  string
		outVals []interface{}
	}{
		{
			in: []Comparable{
				Eq(map[string]interface{}{
					"a": "a",
					"b": "b",
				}),
				Ne(map[string]interface{}{
					"foo": 1,
					"sex": "male",
				}),
				In(map[string][]interface{}{
					"qq": {7, 8, 9},
				}),
			},
			outStr:  "(a=? and b=? and foo!=? and sex!=? and qq in (?,?,?))",
			outVals: []interface{}{"a", "b", 1, "male", 7, 8, 9},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		actualStr, actualVals := whereConnector("and", tc.in...)
		ass.Equal(tc.outStr, actualStr)
		ass.Equal(tc.outVals, actualVals)
	}
}

func TestBuildInsert(t *testing.T) {
	var data = []struct {
		table      string
		stable     string
		tags       []interface{}
		insertType insertType
		data       [][]interface{}
		outStr     string
		outVals    []interface{}
		outErr     error
	}{
		{
			table:      "tb1",
			insertType: commonInsert,
			stable:     "stable1",
			tags: []interface{}{
				1, "2", 3.5,
			},
			data: [][]interface{}{
				{
					1,
					2,
				},
				{
					3,
					4,
				},
				{
					5,
					6,
				},
			},
			outStr:  "insert into tb1 using stable1 tags(?,?,?) values (?,?),(?,?),(?,?)",
			outVals: []interface{}{1, "2", 3.5, 1, 2, 3, 4, 5, 6},
			outErr:  nil,
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		actualStr, actualVals, err := buildInsert(tc.table, tc.stable, tc.tags, tc.data, tc.insertType)
		ass.Equal(tc.outErr, err)
		ass.Equal(tc.outStr, actualStr)
		ass.Equal(tc.outVals, actualVals)
	}
}

func TestBuildSelect(t *testing.T) {
	var data = []struct {
		table      string
		fields     []string
		conditions []Comparable
		groupBy    string
		orderBy    string
		limit      *eleLimit
		sLimit     *eleLimit
		fill       string
		interval   string
		lockMode   string
		outStr     string
		outVals    []interface{}
		outErr     error
	}{
		{
			table:  "tb",
			fields: []string{"foo", "bar"},
			conditions: []Comparable{
				Eq(map[string]interface{}{
					"foo": 1,
					"bar": 2,
				}),
				In(map[string][]interface{}{
					"qq": {4, 5, 6},
				}),
				OrWhere([]Comparable{
					NestWhere([]Comparable{
						Eq(map[string]interface{}{
							"aa": 3,
						}),
						Eq(map[string]interface{}{
							"bb": 4,
						}),
					}),
					NestWhere([]Comparable{
						Eq(map[string]interface{}{
							"cc": 7,
						}),
						Eq(map[string]interface{}{
							"dd": 8,
						}),
					}),
				}),
			},
			groupBy: "",
			orderBy: "foo DESC,baz ASC",
			limit: &eleLimit{
				begin: 10,
				step:  20,
			},

			outErr:  nil,
			outStr:  "select foo,bar from tb where (bar=? and foo=? and qq in (?,?,?) and ((aa=? and bb=?) or (cc=? and dd=?))) order by foo DESC,baz ASC limit ?,?",
			outVals: []interface{}{2, 1, 4, 5, 6, 3, 4, 7, 8, 10, 20},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		cond, vals, err := buildSelect(tc.table, tc.fields, tc.groupBy, tc.orderBy, tc.sLimit, tc.limit, tc.interval, tc.fill, tc.conditions...)
		ass.Equal(tc.outErr, err)
		ass.Equal(tc.outStr, cond)
		ass.Equal(tc.outVals, vals)
	}
}
