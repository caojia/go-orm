package orm

import "testing"

func Test_addLimit(t *testing.T) {
	type args struct {
		sql         string
		limitStatus int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"t1", args{sql: `select * from test_orm_a123 where test_id = ? `, limitStatus: 0}, `select * from test_orm_a123 where test_id = ? LIMIT 2000 `},
		{"t2", args{
			sql: `
			select * from test_orm_a123 where test_id = ?;
			`,
			limitStatus: 0}, `select * from test_orm_a123 where test_id = ? LIMIT 2000 `},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := addLimit(tt.args.sql, tt.args.limitStatus); got != tt.want {
				t.Errorf("addLimit() = %v, want %v", got, tt.want)
			}
		})
	}
}
