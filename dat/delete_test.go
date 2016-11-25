package dat

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkDeleteSql(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		DeleteFrom("alpha").Where("a = $1", "b").ToSQL()
	}
}

func TestDeleteAllToSql(t *testing.T) {
	sql, _, err := DeleteFrom("a").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "DELETE FROM a")
}

func TestDeleteSingleToSql(t *testing.T) {
	sql, args, err := DeleteFrom("a").Where("id = $1", 1).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "DELETE FROM a WHERE (id = $1)")
	assert.Equal(t, args, []interface{}{1})
}

func TestDeleteTenStaringFromTwentyToSql(t *testing.T) {
	sql, _, err := DeleteFrom("a").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "DELETE FROM a")
}

func TestDeleteWhereExprSql(t *testing.T) {
	expr := Expr("id=$1", 100)
	sql, args, err := DeleteFrom("a").Where("foo = $1", "bar").Where(expr).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, `DELETE FROM a WHERE (foo = $1) AND (id=$2)`)
	assert.Exactly(t, args, []interface{}{"bar", 100})
}
