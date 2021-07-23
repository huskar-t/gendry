package builder

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_BuildInsert(t *testing.T) {
	ass := assert.New(t)
	type inStruct struct {
		table   string
		setData [][]interface{}
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				setData: [][]interface{}{
					{
						23,
						"bar",
					},
				},
			},
			out: outStruct{
				cond: "insert into tb values (?,?)",
				vals: []interface{}{23, "bar"},
				err:  nil,
			},
		},
	}
	for _, tc := range data {
		cond, vals, err := BuildInsert(tc.in.table, tc.in.setData)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}
func Test_BuildBuildInsertStable(t *testing.T) {
	ass := assert.New(t)
	type inStruct struct {
		table   string
		sTable  string
		tags    []interface{}
		setData [][]interface{}
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table:  "tb",
				sTable: "stb",
				tags:   []interface{}{"t1", 5, 12.3},
				setData: [][]interface{}{
					{
						23,
						"bar",
					},
				},
			},
			out: outStruct{
				cond: "insert into tb using stb tags(?,?,?) values (?,?)",
				vals: []interface{}{"t1", 5, 12.3, 23, "bar"},
				err:  nil,
			},
		},
	}
	for _, tc := range data {
		cond, vals, err := BuildInsertStable(tc.in.table, tc.in.sTable, tc.in.tags, tc.in.setData)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}
func Test_BuildSelect(t *testing.T) {
	type inStruct struct {
		table  string
		where  map[string]interface{}
		fields []string
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"foo":      "bar",
					"qq":       "tt",
					"age in":   []interface{}{1, 3, 5, 7, 9},
					"vx":       []interface{}{1, 3, 5},
					"faith <>": "Muslim",
					"_or": []map[string]interface{}{
						{
							"aa": 11,
							"bb": "xswl",
						},
						{
							"cc":    "234",
							"dd in": []interface{}{7, 8},
							"_or": []map[string]interface{}{
								{
									"neeest_ee <>": "dw42",
									"neeest_ff in": []interface{}{34, 59},
								},
								{
									"neeest_gg":        1259,
									"neeest_hh not in": []interface{}{358, 1245},
								},
							},
						},
					},
					"_orderby": "age DESC,score ASC",
					"_groupby": "department",
					"_limit":   []uint{0, 100},
				},
				fields: []string{"id", "name", "age"},
			},
			out: outStruct{
				cond: "select id,name,age from tb where (((aa=? and bb=?) or (((neeest_ff in (?,?) and neeest_ee!=?) or (neeest_gg=? and neeest_hh not in (?,?))) and cc=? and dd in (?,?))) and foo=? and qq=? and age in (?,?,?,?,?) and vx in (?,?,?) and faith!=?) group by department order by age DESC,score ASC limit ?,?",
				vals: []interface{}{11, "xswl", 34, 59, "dw42", 1259, 358, 1245, "234", 7, 8, "bar", "tt", 1, 3, 5, 7, 9, 1, 3, 5, "Muslim", 0, 100},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"name like": "%123",
				},
				fields: nil,
			},
			out: outStruct{
				cond: `select * from tb where (name like ?)`,
				vals: []interface{}{"%123"},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"name": "caibirdme",
				},
				fields: nil,
			},
			out: outStruct{
				cond: "select * from tb where (name=?)",
				vals: []interface{}{"caibirdme"},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"foo":      "bar",
					"_orderby": "  ",
				},
				fields: nil,
			},
			out: outStruct{
				cond: "select * from tb where (foo=?)",
				vals: []interface{}{"bar"},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"_slimit":  []uint{0, 100},
					"_groupby": "fool",
				},
				fields: []string{"fool", "bar"},
			},
			out: outStruct{
				cond: "select fool,bar from tb group by fool slimit ?,?",
				vals: []interface{}{0, 100},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"_interval": &Interval{
						Value: 1,
						Unit:  Day,
					},
				},
				fields: []string{"avg(t1)"},
			},
			out: outStruct{
				cond: "select avg(t1) from tb interval(1d)",
				vals: nil,
				err:  nil,
			},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		cond, vals, err := BuildSelect(tc.in.table, tc.in.where, tc.in.fields)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}

func BenchmarkBuildSelect_Sequelization(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _, err := BuildSelect("tb", map[string]interface{}{
			"foo":      "bar",
			"qq":       "tt",
			"age in":   []interface{}{1, 3, 5, 7, 9},
			"faith <>": "Muslim",
			"_orderby": "age DESC",
			"_groupby": "department",
			"_limit":   []uint{0, 100},
		}, []string{"a", "b", "c"})
		if err != nil {
			b.FailNow()
		}
	}
}

func BenchmarkBuildSelect_Parallel(b *testing.B) {
	expectCond := "select * from tb where (foo=? and qq=? and age in (?,?,?,?,?) and faith!=?) group by department order by age DESC limit ?,?"
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cond, _, _ := BuildSelect("tb", map[string]interface{}{
				"foo":      "bar",
				"qq":       "tt",
				"age in":   []interface{}{1, 3, 5, 7, 9},
				"faith <>": "Muslim",
				"_orderby": "age DESC",
				"_groupby": "department",
				"_limit":   []uint{0, 100},
			}, nil)
			if cond != expectCond {
				b.Fatalf("should be %s but %s\n", expectCond, cond)
			}
		}
	})
}

func TestNamedQuery(t *testing.T) {
	var testData = []struct {
		sql  string
		data map[string]interface{}
		cond string
		vals []interface{}
		err  error
	}{
		{
			sql: `select * from tb where name={{name}}`,
			data: map[string]interface{}{
				"age": 24,
			},
			cond: "",
			vals: nil,
			err:  errors.New("name not found"),
		},
		{
			sql:  `select * from tb where name=hello`,
			data: nil,
			cond: "select * from tb where name=hello",
			vals: nil,
			err:  nil,
		},
		{
			sql: `select * from tb where name={{name}} and age<{{age}}`,
			data: map[string]interface{}{
				"age": 24,
			},
			cond: "",
			vals: nil,
			err:  errors.New("name not found"),
		},
		{
			sql: `select * from tb where name={{name}} and age<>{{age}}`,
			data: map[string]interface{}{
				"name": "caibirdme",
				"age":  24,
			},
			cond: `select * from tb where name=? and age<>?`,
			vals: []interface{}{"caibirdme", 24},
			err:  nil,
		},
		{
			sql: `select * from tb where name={{name}} and age in {{age}}`,
			data: map[string]interface{}{
				"name": "caibirdme",
				"age":  []int{1, 2, 3},
			},
			cond: `select * from tb where name=? and age in (?,?,?)`,
			vals: []interface{}{"caibirdme", 1, 2, 3},
			err:  nil,
		},
		{
			sql: `select * from tb where name={{name}} and age in (select m_age from anothertb where m_age>{{m_age}})`,
			data: map[string]interface{}{
				"name":  "caibirdme",
				"m_age": 88.9,
			},
			cond: `select * from tb where name=? and age in (select m_age from anothertb where m_age>?)`,
			vals: []interface{}{"caibirdme", 88.9},
			err:  nil,
		},
		{
			sql: `select * from tb where age in {{some}} and other in {{some}}`,
			data: map[string]interface{}{
				"some": []float64{24.0, 28.7},
			},
			cond: "select * from tb where age in (?,?) and other in (?,?)",
			vals: []interface{}{24.0, 28.7, 24.0, 28.7},
			err:  nil,
		},
		{
			sql: `select a.name,a.age from tb1 as a join tb2 as b on a.id=b.id where a.age>{{age}} and b.age<{{foo}} order by a.name desc limit {{limit}}`,
			data: map[string]interface{}{
				"age":   20,
				"foo":   30,
				"limit": 40,
			},
			cond: "select a.name,a.age from tb1 as a join tb2 as b on a.id=b.id where a.age>? and b.age<? order by a.name desc limit ?",
			vals: []interface{}{20, 30, 40},
			err:  nil,
		},
		{
			sql: `select * from tb where age in {{age}}`,
			data: map[string]interface{}{
				"age": []int{1},
			},
			cond: `select * from tb where age in (?)`,
			vals: []interface{}{1},
			err:  nil,
		},
		{
			sql: `select {{foo}},{{bar}} from tb where age={{age}} and address in {{addr}}`,
			data: map[string]interface{}{
				"foo":  "f1",
				"bar":  "f2",
				"age":  10,
				"addr": []string{"beijing", "shanghai", "chengdu"},
			},
			cond: `select ?,? from tb where age=? and address in (?,?,?)`,
			vals: []interface{}{"f1", "f2", 10, "beijing", "shanghai", "chengdu"},
			err:  nil,
		},
	}
	ass := assert.New(t)
	for _, tc := range testData {
		cond, vals, err := NamedQuery(tc.sql, tc.data)
		if !ass.Equal(tc.err, err) {
			return
		}
		ass.Equal(tc.cond, cond)
		ass.Equal(tc.vals, vals)
	}
}

func Test_BuildIN(t *testing.T) {
	type inStruct struct {
		table  string
		where  map[string]interface{}
		fields []string
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"foo":      "bar",
					"qq":       "tt",
					"age in":   []int{1, 3, 5, 7, 9},
					"faith <>": "Muslim",
					"_orderby": "age DESC",
					"_groupby": "department",
				},
				fields: []string{"id", "name", "age"},
			},
			out: outStruct{
				cond: "select id,name,age from tb where (foo=? and qq=? and age in (?,?,?,?,?) and faith!=?) group by department order by age DESC",
				vals: []interface{}{"bar", "tt", 1, 3, 5, 7, 9, "Muslim"},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"foo":    "bar",
					"age IN": []int{1, 3, 5, 7, 9},
				},
				fields: []string{"id", "name", "age"},
			},
			out: outStruct{
				cond: "select id,name,age from tb where (foo=? and age in (?,?,?,?,?))",
				vals: []interface{}{"bar", 1, 3, 5, 7, 9},
				err:  nil,
			},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		cond, vals, err := BuildSelect(tc.in.table, tc.in.where, tc.in.fields)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}

func Benchmark_BuildIN(b *testing.B) {
	where := map[string]interface{}{
		"age": []uint64{1, 3, 5, 7, 9},
	}
	for i := 0; i < b.N; i++ {
		convertWhereMapToWhereMapSlice(where, opIn)
	}
}

func Test_BuildOrderBy(t *testing.T) {
	type inStruct struct {
		table  string
		where  map[string]interface{}
		fields []string
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"foo":      "bar",
					"_orderby": "age DESC,id ASC",
				},
				fields: []string{"id", "name", "age"},
			},
			out: outStruct{
				cond: "select id,name,age from tb where (foo=?) order by age DESC,id ASC",
				vals: []interface{}{"bar"},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"foo":      "bar",
					"_orderby": "Rand()",
				},
				fields: []string{"id", "name", "age"},
			},
			out: outStruct{
				cond: "select id,name,age from tb where (foo=?) order by Rand()",
				vals: []interface{}{"bar"},
				err:  nil,
			},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		cond, vals, err := BuildSelect(tc.in.table, tc.in.where, tc.in.fields)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}

func Test_Where_Null(t *testing.T) {
	type inStruct struct {
		table  string
		where  map[string]interface{}
		fields []string
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"aa": IsNotNull,
				},
				fields: []string{"id", "name"},
			},
			out: outStruct{
				cond: "select id,name from tb where (aa is not null)",
				vals: nil,
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"aa":  IsNotNull,
					"foo": "bar",
				},
				fields: []string{"id", "name", "age"},
			},
			out: outStruct{
				cond: "select id,name,age from tb where (foo=? and aa is not null)",
				vals: []interface{}{"bar"},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"aa":  IsNull,
					"foo": "bar",
				},
				fields: []string{"id", "name", "age"},
			},
			out: outStruct{
				cond: "select id,name,age from tb where (foo=? and aa is null)",
				vals: []interface{}{"bar"},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"aa":  IsNull,
					"foo": "bar",
					"bb":  IsNotNull,
				},
				fields: []string{"id", "name", "age"},
			},
			out: outStruct{
				cond: "select id,name,age from tb where (foo=? and aa is null and bb is not null)",
				vals: []interface{}{"bar"},
				err:  nil,
			},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		cond, vals, err := BuildSelect(tc.in.table, tc.in.where, tc.in.fields)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}

func TestBuildSelect_Limit(t *testing.T) {
	var testCase = []struct {
		limit  []uint
		err    error
		expect []interface{}
	}{
		{
			limit:  []uint{10, 20},
			err:    nil,
			expect: []interface{}{10, 20},
		},
		{
			limit:  []uint{0, 1},
			err:    nil,
			expect: []interface{}{0, 1},
		},
		{
			limit:  []uint{1},
			err:    nil,
			expect: []interface{}{0, 1},
		},
		{
			limit:  []uint{20, 10},
			err:    nil,
			expect: []interface{}{20, 10},
		},
		{
			limit:  []uint{},
			err:    errLimitValueLength,
			expect: nil,
		},
		{
			limit:  []uint{1, 2, 3},
			err:    errLimitValueLength,
			expect: nil,
		},
	}
	ass := assert.New(t)
	for _, tc := range testCase {
		cond, vals, err := BuildSelect("tb", map[string]interface{}{
			"_limit": tc.limit,
		}, nil)
		ass.Equal(tc.err, err)
		if tc.err == nil {
			ass.Equal(`select * from tb limit ?,?`, cond, "where=%+v", tc.limit)
			ass.Equal(tc.expect, vals)
		}
	}
}

func Test_NotIn(t *testing.T) {
	table := "some_table"
	fields := []string{"name", "age", "sex"}
	where := []map[string]interface{}{
		{
			"city in":            []string{"beijing", "shanghai"},
			"age >":              35,
			"address":            IsNotNull,
			" hobbies not in   ": []string{"baseball", "swim", "running"},
			"_groupby":           "department",
			"_orderby":           "bonus DESC",
		},
		{
			"city IN":            []string{"beijing", "shanghai"},
			"age >":              35,
			"address":            IsNotNull,
			" hobbies not IN   ": []string{"baseball", "swim", "running"},
			"_groupby":           "department",
			"_orderby":           "bonus DESC",
		},
	}

	expectCond := `select name,age,sex from some_table where (city in (?,?) and hobbies not in (?,?,?) and age>? and address is not null) group by department order by bonus DESC`
	expectVals := []interface{}{"beijing", "shanghai", "baseball", "swim", "running", 35}

	ass := assert.New(t)
	for _, w := range where {
		cond, vals, err := BuildSelect(table, w, fields)
		ass.NoError(err)
		ass.Equal(expectCond, cond)
		ass.Equal(expectVals, vals)
	}
}

func TestBuildBetween(t *testing.T) {
	table := "tb"
	fields := []string{"foo", "bar"}
	where := []map[string]interface{}{
		{
			"city in ":    []string{"beijing", "chengdu"},
			"age between": []int{10, 30},
			"name":        "caibirdme",
		},
		{
			"city IN ":    []string{"beijing", "chengdu"},
			"age between": []int{10, 30},
			"name":        "caibirdme",
		},
	}

	expectCond := "select foo,bar from tb where (name=? and city in (?,?) and (age between ? and ?))"
	expectVals := []interface{}{"caibirdme", "beijing", "chengdu", 10, 30}

	ass := assert.New(t)
	for _, w := range where {
		cond, vals, err := BuildSelect(table, w, fields)
		ass.NoError(err)
		ass.Equal(expectCond, cond)
		ass.Equal(expectVals, vals)
	}
}

func TestBuildNotBetween(t *testing.T) {
	table := "tb"
	fields := []string{"foo", "bar"}
	where := []map[string]interface{}{
		{
			"city in ":        []string{"beijing", "chengdu"},
			"age not between": []int{10, 30},
			"name":            "caibirdme",
			"_limit":          []uint{10, 20},
		},
		{
			"city IN ":        []string{"beijing", "chengdu"},
			"age not between": []int{10, 30},
			"name":            "caibirdme",
			"_limit":          []uint{10, 20},
		},
	}

	expectCond := "select foo,bar from tb where (name=? and city in (?,?) and (age not between ? and ?)) limit ?,?"
	expectVals := []interface{}{"caibirdme", "beijing", "chengdu", 10, 30, 10, 20}

	ass := assert.New(t)
	for _, w := range where {
		cond, vals, err := BuildSelect(table, w, fields)
		ass.NoError(err)
		ass.Equal(expectCond, cond)
		ass.Equal(expectVals, vals)
	}
}

func TestBuildCombinedBetween(t *testing.T) {
	table := "tb"
	fields := []string{"foo", "bar"}
	where := []map[string]interface{}{
		{
			"city in ":        []string{"beijing", "chengdu"},
			"age not between": []int{10, 30},
			"name":            "caibirdme",
			"score between":   []float64{3.5, 7.2},
			"_limit":          []uint{10, 20},
		},
		{
			"city IN ":        []string{"beijing", "chengdu"},
			"age not between": []int{10, 30},
			"name":            "caibirdme",
			"score between":   []float64{3.5, 7.2},
			"_limit":          []uint{10, 20},
		},
	}

	expectCond := "select foo,bar from tb where (name=? and city in (?,?) and (score between ? and ?) and (age not between ? and ?)) limit ?,?"
	expectVals := []interface{}{"caibirdme", "beijing", "chengdu", 3.5, 7.2, 10, 30, 10, 20}

	ass := assert.New(t)
	for _, w := range where {
		cond, vals, err := BuildSelect(table, w, fields)
		ass.NoError(err)
		ass.Equal(expectCond, cond)
		ass.Equal(expectVals, vals)
	}
}

func TestLike(t *testing.T) {
	type inStruct struct {
		table  string
		where  []map[string]interface{}
		fields []string
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				where: []map[string]interface{}{
					{
						"bar like": "haha%",
						"baz like": "%some",
						"foo":      1,
					},
					{
						"bar like": "haha%",
						"baz like": "%some",
						"foo":      1,
					},
				},
				fields: nil,
			},
			out: outStruct{
				cond: `select * from tb where (foo=? and bar like ? and baz like ?)`,
				vals: []interface{}{1, "haha%", "%some"},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: []map[string]interface{}{
					{
						"bar like": "haha%",
						"baz like": "%some",
						"foo":      1,
						"age in":   []interface{}{1, 3, 5, 7, 9},
					},
					{
						"bar like": "haha%",
						"baz like": "%some",
						"foo":      1,
						"age IN":   []interface{}{1, 3, 5, 7, 9},
					},
				},
				fields: nil,
			},
			out: outStruct{
				cond: `select * from tb where (foo=? and age in (?,?,?,?,?) and bar like ? and baz like ?)`,
				vals: []interface{}{1, 1, 3, 5, 7, 9, "haha%", "%some"},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: []map[string]interface{}{
					{
						"name like": "%James",
					},
					{
						"name like": "%James",
					},
				},
				fields: []string{"name"},
			},
			out: outStruct{
				cond: `select name from tb where (name like ?)`,
				vals: []interface{}{"%James"},
				err:  nil,
			},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		for _, w := range tc.in.where {
			cond, vals, err := BuildSelect(tc.in.table, w, tc.in.fields)
			ass.Equal(tc.out.err, err)
			ass.Equal(tc.out.cond, cond)
			ass.Equal(tc.out.vals, vals)
		}
	}
}

func TestNotLike(t *testing.T) {
	table := "tb"
	where := []map[string]interface{}{
		{
			"name  not    like  ": "%ny",
		},
		{
			"name  not    like  ": "%ny",
		},
	}

	expectCond := "select * from tb where (name not like ?)"
	expectVals := []interface{}{"%ny"}

	ass := assert.New(t)
	for _, w := range where {
		cond, vals, err := BuildSelect(table, w, nil)
		ass.NoError(err)
		ass.Equal(expectCond, cond)
		ass.Equal(expectVals, vals)
	}
}

func TestNotLike_1(t *testing.T) {
	table := "tb"
	where := []map[string]interface{}{
		{
			"name  not like  ": "%ny",
			"age":              20,
		},
		{
			"name  not like  ": "%ny",
			"age":              20,
		},
	}

	expectCond := "select * from tb where (age=? and name not like ?)"
	expectVals := []interface{}{20, "%ny"}

	ass := assert.New(t)
	for _, w := range where {
		cond, vals, err := BuildSelect(table, w, nil)
		ass.NoError(err)
		ass.Equal(expectCond, cond)
		ass.Equal(expectVals, vals)
	}
}

func TestFixBug_insert_quote_field(t *testing.T) {
	cond, vals, err := BuildInsert("tb", [][]interface{}{
		{
			1,
			2,
			3, // I know this is forbidden, but just for test
		},
	})
	ass := assert.New(t)
	ass.NoError(err)
	ass.Equal("insert into tb values (?,?,?)", cond)
	ass.Equal([]interface{}{1, 2, 3}, vals)
}
