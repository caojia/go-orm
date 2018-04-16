package orm

import (
	"github.com/magiconair/properties/assert"
	"testing"
)

func TestAddLimit(t *testing.T) {
	str := addLimit(`select * from test_orm_a123 where test_id = ?   `, 0)
	assert.Equal(t, "select * from test_orm_a123 where test_id = ?    LIMIT 2000 ", str)

	str = addLimit(`select show from test_orm_a123 where test_id = ?   `, 0)
	assert.Equal(t, "select show from test_orm_a123 where test_id = ?    LIMIT 2000 ", str)

	str = addLimit(`show tables`, 0)
	assert.Equal(t, "show tables", str)
}
