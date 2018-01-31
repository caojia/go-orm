package orm

import (
	"testing"
	"log"
)

func TestAddLimit(t *testing.T) {
	str:=addLimit(`select * from test_orm_a123 where test_id = ?   `,0)
	log.Println(str)
}