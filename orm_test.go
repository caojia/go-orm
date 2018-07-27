package orm

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/magiconair/properties/assert"
)

type TestOrmA123 struct {
	TestID      int64 `json:"test_id" pk:"true" ai:"true" db:"test_id,ai,pk"`
	OtherId     int64
	Description string
	Name        sql.NullString
	StartDate   time.Time
	EndDate     time.Time
	TestOrmDId  int64
	OrmB        *TestOrmB999   `or:"has_one" table:"test_orm_b999"`
	OrmCs       []*TestOrmC111 `or:"has_many" table:"test_orm_c111"`
	OrmD        *TestOrmD222   `or:"belongs_to" table:"test_orm_d222"`
	CreatedAt   time.Time      `ignore:"true"`
	UpdatedAt   time.Time      `ignore:"true"`
}

type TestOrmB999 struct {
	NoAiId      int64 `pk:"true"`
	Description string
	TestID      int64     `db:"test_id"`
	CreatedAt   time.Time `ignore:"true"`
	UpdatedAt   time.Time `ignore:"true"`
}

type TestOrmC111 struct {
	TestOrmCId int64 `db:"ai,pk"`
	TestID     int64 `db:"test_id"`
	Name       string
}

type TestOrmD222 struct {
	TestOrmDId int64 `pk:"true" ai:"true"`
	Name       string
}

type TestOrmE333 struct {
	TestOrmEId  int64 `pk:"true" ai:"true"`
	Name        string
	Description sql.NullString
	VInt64      int64
	VInt        int
	VUint64     uint64
	VUint       uint
	VBoolean    bool
	VBigDecimal float64
	VFloat      float64
	StartTime   time.Time
	CreatedAt   time.Time `ignore:"true"`
}

type TestOrmF123 struct {
	Id   int64 `pk:"true" ai:"true"`
	Name string
}

func (obj TestOrmF123) TableName() string {
	return "orm_f"
}

func oneTestScope(fn func(orm *ORM, testTableName string)) {
	orm := NewORM("root@/orm_test?parseTime=true&loc=Local")
	orm.TruncateTables()
	_, err := orm.Exec(`
        CREATE TABLE IF NOT EXISTS test_orm_a123 (
          test_id BIGINT(20) NOT NULL AUTO_INCREMENT,
          test_orm_d_id BIGINT(20) NOT NULL,
          other_id BIGINT(20) NOT NULL,
          description VARCHAR(1024) NOT NULL,
          name VARCHAR(50) NULL,
          start_date DATETIME NOT NULL,
          end_date DATETIME NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (test_id))
        ENGINE = InnoDB;`)
	if err != nil {
		log.Printf("error %+v", err)
	}

	_, err = orm.Exec(`
        CREATE TABLE IF NOT EXISTS test_orm_b999 (
          no_ai_id BIGINT(20) NOT NULL,
          description VARCHAR(1024) NOT NULL,
          end_date TIMESTAMP NOT NULL,
          test_id BIGINT(20) NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (no_ai_id),
          INDEX test_id (test_id ASC))
        ENGINE = InnoDB;`)
	if err != nil {
		log.Printf("error %+v", err)
	}

	_, err = orm.Exec(`
        CREATE TABLE IF NOT EXISTS test_orm_c111 (
          test_orm_c_id BIGINT(20) NOT NULL AUTO_INCREMENT,
          name VARCHAR(1024) NOT NULL,
          test_id BIGINT(20) NOT NULL,
          PRIMARY KEY (test_orm_c_id),
          INDEX test_id (test_id ASC))
        ENGINE = InnoDB;`)
	if err != nil {
		log.Printf("error %+v", err)
	}

	_, err = orm.Exec(`
        CREATE TABLE IF NOT EXISTS test_orm_d222 (
          test_orm_d_id BIGINT(20) NOT NULL AUTO_INCREMENT,
          name VARCHAR(1024) NOT NULL,
          PRIMARY KEY (test_orm_d_id))
        ENGINE = InnoDB;`)
	if err != nil {
		log.Printf("error %+v", err)
	}

	_, err = orm.Exec(`
	CREATE TABLE IF NOT EXISTS test_orm_e333 (
		test_orm_e_id BIGINT NOT NULL AUTO_INCREMENT,
		name VARCHAR(1024) NOT NULL,
		description VARCHAR(1024) NULL,
		v_int64 BIGINT NOT NULL,
		v_int INT NOT NULL,
		v_uint64 BIGINT UNSIGNED NOT NULL,
		v_uint INT UNSIGNED NOT NULL,
		v_boolean TINYINT(1) NOT NULL,
		v_big_decimal DECIMAL(12, 7) NOT NULL,
		v_float FLOAT NOT NULL,
		start_time DATETIME NOT NULL DEFAULT '1970-01-01 00:00:00',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		primary key (test_orm_e_id)
	)
	`)
	if err != nil {
		log.Printf("error %+v\n", err)
	}

	_, err = orm.Exec(`
	CREATE TABLE IF NOT EXISTS orm_f (
		id BIGINT NOT NULL AUTO_INCREMENT,
		name VARCHAR(1024) NOT NULL,
		primary key (id)
	)
	`)
	if err != nil {
		log.Printf("error %+v\n", err)
	}

	defer orm.Exec("DROP TABLE IF EXISTS test_orm_b999;")
	defer orm.Exec("DROP TABLE IF EXISTS test_orm_a123;")
	defer orm.Exec("DROP TABLE IF EXISTS test_orm_c111;")
	defer orm.Exec("DROP TABLE IF EXISTS test_orm_d222;")
	defer orm.Exec("DROP TABLE IF EXISTS test_orm_e333;")
	defer orm.Exec("DROP TABLE IF EXISTS orm_f;")
	fn(orm, "test_orm_a123")
}
func TestSelectArr(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		for i := 10; i > 0; i-- {
			testObj := &TestOrmA123{
				OtherId:     1,
				TestOrmDId:  0,
				Description: "update test ",
				StartDate:   time.Now(),
				EndDate:     time.Now(),
			}
			err := orm.Insert(testObj)
			if err != nil {
				t.Error(err)
			}
		}
		var arr []int
		err := orm.Select(&arr, "select test_id from test_orm_a123 where other_id = 1")
		if err != nil {
			t.Error(err)
		}
		log.Println(arr)
	})
}

func TestInsert(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {

		testObj := &TestOrmA123{
			OtherId:     1,
			TestOrmDId:  0,
			Description: "update test ",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		err := orm.InsertWithTable(testObj, "test_orm_a123")
		if err != nil {
			t.Error(err)
		}

		var arr []int
		if orm.Select(&arr, "select test_id from test_orm_a123 where other_id = 1") != nil {
			t.Error(err)
		}
		log.Println(arr)
	})
}

func TestSelectInt32(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		testObj := &TestOrmA123{
			OtherId:     1,
			TestOrmDId:  0,
			Description: "update test ",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}

		err := orm.Insert(testObj)
		if err != nil {
			t.Error(err)
		}
		var arr []int32
		err = orm.Select(&arr, "select other_id from test_orm_a123 where other_id = ?", 1)
		if err != nil {
			t.Error(err)
		}
		log.Println(err)
	})
}
func TestORMExecIN(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		for i := 10; i > 0; i-- {
			testObj := &TestOrmA123{
				OtherId:     1,
				TestOrmDId:  0,
				Description: "update test ",
				StartDate:   time.Now(),
				EndDate:     time.Now(),
			}
			err := orm.Insert(testObj)
			if err != nil {
				t.Error(err)
			}
		}
		sql := "update test_orm_a123 set description = 'update' where test_id in (??)"
		testId := []int{1, 2, 3, 4, 5}
		res, err := orm.Exec(sql, testId)
		if err != nil {
			t.Error(err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, int(n), 5)
		var testOrmA123 []*TestOrmA123
		err = orm.Select(&testOrmA123, `select * from test_orm_a123 where description = ?`, "update")
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, len(testOrmA123), 5)
	})
}

func TestORMUpdate(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		testObj := &TestOrmA123{
			OtherId:     1,
			TestOrmDId:  0,
			Description: "update test ",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		err := orm.Insert(testObj)
		if err != nil {
			t.Error(err)
		}
		testObj.Description = "update"
		err = orm.UpdateByPK(testObj)
		if err != nil {
			t.Error(err)
		}
		var loadedObj TestOrmA123
		if err = orm.SelectByPK(&loadedObj, testObj.TestID); err != nil {
			t.Error(err)
			if len(loadedObj.Description) == 0 {
				t.Error(loadedObj)
			}
			return
		} else if loadedObj.Description != "update" {
			t.Error(loadedObj)
			return
		}
	})
}

func TestOrmInsertOrUpdate(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		testObj1 := &TestOrmA123{
			OtherId:     1,
			TestOrmDId:  0,
			Description: "update test ",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		err := orm.Insert(testObj1)
		if err != nil {
			t.Error(err)
		}
		if testObj1.TestID != 1 {
			t.Fatal("test id should be 1")
		}
		testObj1.TestID = 1
		testObj1.Description = "update"
		err = orm.InsertOrUpdate(testObj1, []string{"description"})
		if err != nil {
			t.Error(err)
		}
		testObj2 := &TestOrmA123{
			OtherId:     1,
			TestOrmDId:  0,
			Description: "update test",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		err = orm.Insert(testObj2)
		if err != nil {
			t.Error(err)
		}
		var loadobj []*TestOrmA123
		err = orm.Select(&loadobj, "select * from test_orm_a123 where other_id = ?", 1)
		if err != nil {
			t.Error(err)
		}
		if len(loadobj) != 2 {
			t.Error(len(loadobj))
		}
		t3 := &TestOrmA123{}
		err = orm.SelectOne(t3, "select * from test_orm_a123 where test_id = ?", 1)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, t3.Description, "update")
		testObj4 := &TestOrmA123{
			OtherId:     1,
			TestOrmDId:  0,
			Description: "update test ",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		testObj4.TestID = 0
		log.Println("test_id", testObj4.TestID)
		//test case 对0值进行判断
		err = orm.InsertOrUpdate(testObj4, []string{"description"})
		if err != nil {
			t.Error(err)
		}
		if testObj4.TestID != 3 {
			t.Fatal("test id should be 3")
		}
	})
}
func TestORMUpdateFieldsByPK(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		testObj := &TestOrmA123{
			OtherId:     1,
			TestOrmDId:  0,
			Description: "update test ",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		orm.Insert(testObj)
		testObj.Description = "update"
		testObj.OtherId = 1000
		err := orm.UpdateFieldsByPK(testObj, []string{"description", "OtherId"})
		if err != nil {
			t.Error(err)
		}
		var loadedObj TestOrmA123
		if err := orm.SelectByPK(&loadedObj, testObj.TestID); err != nil {
			t.Error(err)
			if len(loadedObj.Description) == 0 {
				t.Error(loadedObj)
			}
			return
		} else if loadedObj.Description != "update" && loadedObj.OtherId != 2 {
			t.Error(loadedObj.OtherId)
			return
		}
	})
}

func TestQueryRawSetAndQueryRaw(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		orm.Exec("delete from test_orm_a123")
		result, _ := orm.SelectRawSet("select * from test_orm_a123", map[string]string{})
		if len(result) != 0 {
			t.Fatalf("should no result%v", result)
		}
		_, data, _ := orm.SelectRaw("select * from test_orm_a123")
		if len(data) != 0 {
			t.Fatalf("should no result%v", data)
		}

		p1 := &TestOrmA123{
			OtherId:     1,
			TestOrmDId:  0,
			Description: "test orm 1",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}

		p2 := &TestOrmA123{
			OtherId:     10,
			TestOrmDId:  0,
			Description: "test orm 2",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}

		ps := make([]interface{}, 0)
		ps = append(ps, p1, p2)

		err := orm.InsertBatch(ps)
		fmt.Println(err)

		result, _ = orm.SelectRawSet("select * from test_orm_a123", map[string]string{})
		fmt.Println("select raw set", result)
		_, data, _ = orm.SelectRaw("select * from test_orm_a123")
		fmt.Println("select raw", data)
		if len(data) != 2 {
			t.Fatalf("should have 2 result")
		}
	})
}

func TestExecParam(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		testObj := &TestOrmA123{
			OtherId:     1,
			TestOrmDId:  0,
			Description: "test orm 1",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		orm.Insert(testObj)
		orm.Insert(&TestOrmA123{
			OtherId:     10,
			TestOrmDId:  0,
			Description: "test orm 2",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		})

		var paramMap map[string]interface{} = map[string]interface{}{
			"otherId":     2,
			"id":          testObj.TestID,
			"description": "lala",
		}
		_, err := orm.ExecWithParam("update "+testTableName+
			" set other_id = #{otherId}, description = #{description} where test_id = #{id}", paramMap)
		if err != nil {
			t.Error("failed to update", err)
		}
		var loadedObj TestOrmA123
		err = orm.SelectByPK(&loadedObj, testObj.TestID)
		if err != nil {
			t.Error("select failed", err)
		}
		if loadedObj.Description != paramMap["description"] || loadedObj.OtherId != 2 {
			t.Error("fields not updated", loadedObj)
		}

		params2 := map[string]interface{}{
			"otherId":     2,
			"description": "test",
		}

		_, err = orm.ExecWithParam("update "+testTableName+
			" set other_id = #{otherId} + 1, description = #{description} where other_id = #{otherId}", params2)
		orm.SelectByPK(&loadedObj, testObj.TestID)

		if loadedObj.Description != params2["description"] || loadedObj.OtherId != 3 {
			t.Error("fields not updated", loadedObj)
		}

		testParam := &TestOrmA123{
			TestID:      testObj.TestID,
			TestOrmDId:  0,
			OtherId:     5,
			Description: "ad",
			Name:        sql.NullString{"Oa", true},
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		_, err = orm.ExecWithParam("update "+testTableName+
			" set other_id = #{OtherId}, description = #{Description}, name = #{Name} where test_id = #{TestID}", testParam)
		if err != nil {
			t.Error(err)
		}
		err = orm.SelectByPK(&loadedObj, testObj.TestID)
		if err != nil {
			t.Error(err)
		}
		if loadedObj.Description != testParam.Description || loadedObj.OtherId != testParam.OtherId ||
			!loadedObj.Name.Valid || loadedObj.Name.String != testParam.Name.String {
			t.Error("fields not updated", loadedObj)
		}

		//add column
		if _, err := orm.Exec("alter table test_orm_a123 add weight int not null default 0"); err != nil {
			t.Error(err)
			return
		}
		if err := orm.SelectByPK(&loadedObj, testObj.TestID); err != nil {
			t.Error(err)
			if len(loadedObj.Description) == 0 {
				t.Error(loadedObj)
			}
			return
		}
		if _, err := orm.Exec("alter table test_orm_a123 drop weight"); err != nil {
			t.Error(err)
			return
		}
	})
}

func TestAutoIncreaseKey(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		testObj := &TestOrmA123{
			OtherId:     1,
			TestOrmDId:  0,
			Description: "test orm",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		orm.Insert(testObj)
		if testObj.TestID != 1 {
			t.Fatal("test id should be 1")
		}

		testObjB := &TestOrmB999{
			NoAiId:      2,
			Description: "aaa",
		}
		orm.Insert(testObjB)
		if testObjB.NoAiId != 2 {
			t.Fatal("NoAiId should be 2")
		}
	})
}
func TestOrmSelect(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		testObj := &TestOrmA123{
			OtherId:     1,
			TestOrmDId:  0,
			Description: "test orm",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		orm.Insert(testObj)
		if testObj.TestID != 1 {
			t.Fatal("test id should be 1")
		}
		orm.Insert(testObj)
		if testObj.TestID != 2 {
			t.Fatal("test id should be 2")
		}
		testObj2 := &TestOrmA123{
			OtherId:     2,
			TestOrmDId:  0,
			Description: "test orm",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		orm.Insert(testObj2)
		var testList []*TestOrmA123
		start := time.Now()
		err := orm.Select(&testList, "select * from test_orm_a123 where  other_id in (??) and test_id in (??)", []int{1, 2}, []int{1, 2, 3})
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if len(testList) != 3 {
			log.Println(testList[0].TestID)
			t.Error(len(testList))
		}
		var testList1 []*TestOrmA123

		err = orm.Select(&testList1, "select * from test_orm_a123 where test_id = ?", 1)
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if len(testList1) != 1 {
			log.Println(testList[0].TestID)
			t.Error(len(testList))
		}
	})
}
func TestOrmHasOneRelation(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		testObj := &TestOrmA123{
			OtherId:     1,
			TestOrmDId:  0,
			Description: "test orm",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		orm.Insert(testObj)
		if testObj.TestID != 1 {
			t.Fatal("test id should be 1")
		}

		testObjB := &TestOrmB999{
			NoAiId:      2,
			TestID:      testObj.TestID,
			Description: "aaa",
		}
		err := orm.Insert(testObjB)
		if testObjB.NoAiId != 2 {
			t.Fatal("NoAiId should be 2")
		}
		if err != nil {
			t.Error(err)
		}

		var testObj2 []*TestOrmA123
		start := time.Now()
		err = orm.Select(&testObj2, "SELECT * FROM test_orm_a123 WHERE test_id = ?", testObj.TestID)
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if testObj2[0].OrmB == nil {
			t.Fatal("should have one ormB")
		}
		if testObj2[0].OrmB.TestID != testObj2[0].TestID || testObj2[0].OrmB.Description != testObjB.Description ||
			testObj2[0].OrmB.NoAiId != testObjB.NoAiId {
			t.Fatal("invalid ormb")
		}

		objA2 := &TestOrmA123{
			OtherId:    2,
			TestOrmDId: 0,
			StartDate:  time.Now(),
			EndDate:    time.Date(2000, 1, 1, 1, 0, 0, 0, time.Local),
		}
		orm.Insert(objA2)
		orm.Insert(&TestOrmB999{
			TestID:      objA2.TestID,
			NoAiId:      3,
			Description: "ormb3",
		})

		orm.Insert(&TestOrmA123{
			OtherId:     33,
			TestOrmDId:  0,
			Description: "no ormb attached",
			StartDate:   time.Now(),
			EndDate:     time.Date(2100, 5, 3, 1, 0, 0, 0, time.Local),
		})

		// insert 10 orm c objects for each orm a
		count := 10
		for TestID := testObj.TestID; TestID <= objA2.TestID; TestID++ {
			for i := 0; i < count; i++ {
				orm.Insert(&TestOrmC111{
					Name:   fmt.Sprintf("%d_orm_c_%d", TestID, i),
					TestID: TestID,
				})
			}
		}
		var loadOrmA1 TestOrmA123
		start = time.Now()
		err = orm.SelectOne(&loadOrmA1, "select * from test_orm_a123 WHERE test_id = ?", testObj.TestID)
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if loadOrmA1.TestID != testObj.TestID || loadOrmA1.OrmB == nil || len(loadOrmA1.OrmCs) != count {
			t.Fatal("incorrect result")
		}
		for _, c := range loadOrmA1.OrmCs {
			//t.Log(c)
			if c.TestID != testObj.TestID {
				t.Fatal("incorrect result")
			}
		}

		var sliceRes []*TestOrmA123
		start = time.Now()
		err = orm.Select(&sliceRes, "SELECT * FROM test_orm_a123")
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if sliceRes == nil || len(sliceRes) != 3 {
			t.Fatal("incorrect result")
		}
		if sliceRes[0].OrmB == nil || sliceRes[1].OrmB == nil || sliceRes[2].OrmB != nil {
			t.Fatal("should have orm b on first 2 result")
		}
		t.Logf("%+v,\n %+v", sliceRes[0].OrmB, sliceRes[1].OrmB)

		if len(sliceRes[0].OrmCs) != count || len(sliceRes[1].OrmCs) != count || len(sliceRes[2].OrmCs) != 0 {
			t.Fatal("incorrect orm c count")
		}

		for _, ormA := range sliceRes {
			for _, c := range ormA.OrmCs {
				//t.Log(c, ormA.TestID)
				if c.TestID != ormA.TestID {
					t.Fatal("incorrect result")
				}
			}
		}
		f := func(ot *ORMTran) error {
			err = ot.SelectOne(&loadOrmA1, "select * from test_orm_a123 WHERE test_id = ?", testObj.TestID)
			if err != nil {
				t.Fatal(err)
			}
			err = orm.Select(&sliceRes, "SELECT * FROM test_orm_a123")
			if err != nil {
				t.Fatal(err)
			}
			for _, ormA := range sliceRes {
				for _, c := range ormA.OrmCs {
					//t.Log(c, ormA.TestID)
					if c.TestID != ormA.TestID {
						t.Fatal("incorrect result")
					}
				}
			}
			return err
		}
		orm.DoTransaction(f)
	})
}

func TestOrmBelongsToRelation(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		testObjD := &TestOrmD222{
			Name: "test d",
		}
		err := orm.Insert(testObjD)
		if err != nil {
			t.Error(err)
		}
		if testObjD.TestOrmDId != 1 {
			t.Fatal("test d id should be 1")
		}

		testObj := &TestOrmA123{
			OtherId:     1,
			Description: "test orm 1",
			TestOrmDId:  testObjD.TestOrmDId,
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		err = orm.Insert(testObj)
		if err != nil {
			t.Error(err)
		}
		if testObj.TestID != 1 {
			t.Fatal("test id should be 1")
		}

		testObjB := &TestOrmB999{
			NoAiId:      2,
			TestID:      testObj.TestID,
			Description: "aaa",
		}
		orm.Insert(testObjB)
		if testObjB.NoAiId != 2 {
			t.Fatal("NoAiId should be 2")
		}

		var testObj2 TestOrmA123
		start := time.Now()
		err = orm.SelectOne(&testObj2, "SELECT * FROM test_orm_a123 WHERE test_id = ?", testObj.TestID)
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if testObj2.OrmB == nil {
			t.Fatal("should have one ormB")
		}
		if testObj2.OrmD == nil {
			t.Fatal("should have one ormd")
		}
		if testObj2.OrmB.TestID != testObj2.TestID || testObj2.OrmB.Description != testObjB.Description ||
			testObj2.OrmB.NoAiId != testObjB.NoAiId {
			t.Fatal("invalid ormb")
		}
		if testObj2.OrmD.TestOrmDId != testObjD.TestOrmDId || testObj2.Name != testObj2.Name {
			t.Fatal("invalid ormd")
		}

		testObjD2 := &TestOrmD222{
			Name: "test d 2",
		}
		orm.Insert(testObjD2)

		objA2 := &TestOrmA123{
			OtherId:    2,
			TestOrmDId: testObjD2.TestOrmDId,
			StartDate:  time.Now(),
			EndDate:    time.Date(2000, 1, 1, 1, 0, 0, 0, time.Local),
		}
		orm.Insert(objA2)
		orm.Insert(&TestOrmB999{
			TestID:      objA2.TestID,
			NoAiId:      3,
			Description: "ormb3",
		})

		orm.Insert(&TestOrmA123{
			OtherId:     33,
			TestOrmDId:  testObjD2.TestOrmDId,
			Description: "no ormb attached",
			StartDate:   time.Now(),
			EndDate:     time.Date(2100, 5, 3, 1, 0, 0, 0, time.Local),
		})

		// insert 10 orm c objects for each orm a
		count := 10
		for TestID := testObj.TestID; TestID <= objA2.TestID; TestID++ {
			for i := 0; i < count; i++ {
				orm.Insert(&TestOrmC111{
					Name:   fmt.Sprintf("%d_orm_c_%d", TestID, i),
					TestID: TestID,
				})
			}
		}
		var loadOrmA1 TestOrmA123
		start = time.Now()
		err = orm.SelectOne(&loadOrmA1, "select * from test_orm_a123 WHERE test_id = ?", testObj.TestID)
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if loadOrmA1.TestID != testObj.TestID || loadOrmA1.OrmB == nil || len(loadOrmA1.OrmCs) != count {
			t.Fatal("incorrect result")
		}
		for _, c := range loadOrmA1.OrmCs {
			//t.Log(c)
			if c.TestID != testObj.TestID {
				t.Fatal("incorrect result")
			}
		}

		var sliceRes []*TestOrmA123
		start = time.Now()
		err = orm.Select(&sliceRes, "SELECT * FROM test_orm_a123")
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if sliceRes == nil || len(sliceRes) != 3 {
			t.Fatal("incorrect result")
		}
		if sliceRes[0].OrmB == nil || sliceRes[1].OrmB == nil || sliceRes[2].OrmB != nil {
			if sliceRes[0] == nil {
				t.Error("one nil")
			}
			if sliceRes[1] == nil {
				t.Error("two err")
			}
			if sliceRes[2] != nil {
				t.Error("a")
			}
			t.Fatal("should have orm b on first 2 result")
		}
		t.Logf("%+v,\n %+v", sliceRes[0].OrmB, sliceRes[1].OrmB)
		if sliceRes[0].OrmD == nil || sliceRes[1].OrmD == nil || sliceRes[2].OrmD == nil {
			t.Fatal("should have orm d for all result")
		}
		t.Logf("%+v,\n %+v,\n %+v", sliceRes[0].OrmD, sliceRes[1].OrmD, sliceRes[2].OrmD)
		if sliceRes[0].OrmD.Name != testObjD.Name || sliceRes[1].OrmD.Name != testObjD2.Name ||
			sliceRes[2].OrmD.Name != testObjD2.Name {
			t.Fatal("incorrect orm d values")
		}

		if len(sliceRes[0].OrmCs) != count || len(sliceRes[1].OrmCs) != count || len(sliceRes[2].OrmCs) != 0 {
			t.Fatal("incorrect orm c count")
		}

		for _, ormA := range sliceRes {
			for _, c := range ormA.OrmCs {
				//t.Log(c, ormA.TestID)
				if c.TestID != ormA.TestID {
					t.Fatal("incorrect result")
				}
			}
		}

		f := func(ot *ORMTran) error {
			err = ot.SelectOne(&loadOrmA1, "select * from test_orm_a123 WHERE test_id = ?", testObj.TestID)
			if err != nil {
				log.Println(err)
				t.Fatal(err)
			}
			err = orm.Select(&sliceRes, "SELECT * FROM test_orm_a123")
			if err != nil {
				t.Fatal(err)
			}
			for _, ormA := range sliceRes {
				for _, c := range ormA.OrmCs {
					//t.Log(c, ormA.TestID)
					if c.TestID != ormA.TestID {
						t.Fatal("incorrect result")
					}
				}
			}
			return err
		}
		orm.DoTransaction(f)

		objA4 := &TestOrmA123{
			OtherId:    2,
			TestOrmDId: 0,
			StartDate:  time.Now(),
			EndDate:    time.Date(2000, 1, 1, 1, 0, 0, 0, time.Local),
		}
		orm.Insert(objA4)
		var loadOrmA4 TestOrmA123
		err = orm.SelectOne(&loadOrmA4, "select * from test_orm_a123 WHERE test_id = ?", objA4.TestID)
		if err != nil {
			t.Fatal(err)
		}
		if objA4.OrmD != nil {
			t.Fatal("obj A 4's ormd should be nil")
		}

	})
}

func TestPanicHandlingInTransaction(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		testObj := &TestOrmA123{
			OtherId:     1,
			TestOrmDId:  0,
			Description: "test orm 1",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		orm.Insert(testObj)
		testObjB := &TestOrmB999{
			NoAiId:      2,
			TestID:      testObj.TestID,
			Description: "aaa",
		}
		orm.Insert(testObjB)
		f := func(o *ORMTran) error {
			o.Exec("update test_orm_a123 set description = 'test'")
			o.Exec("update test_orm_b999 set description = 'b'")
			return nil
		}
		orm.DoTransaction(f)
		var objA TestOrmA123
		orm.SelectOne(&objA, "SELECT * FROM test_orm_a123 WHERE test_id = ?", testObj.TestID)
		if objA.Description != "test" {
			t.Fatal("incorrect description")
		}
		var objB TestOrmB999
		orm.SelectOne(&objB, "SELECT * FROM test_orm_b999 WHERE test_id = ?", testObjB.TestID)
		if objB.Description != "b" {
			t.Fatal("incorrect description")
		}

		func() {
			defer func() {
				perr := recover()
				t.Logf("expected panic here: %v", perr)
				if perr == nil {
					t.Fatal("should be panic")
				}
			}()
			pf := func(o *ORMTran) error {
				o.Exec("update test_orm_a123 set description = 'test2'")
				panic(errors.New("panic test"))
				o.Exec("update test_orm_b999 set description = 'b2'")
				return nil
			}
			orm.DoTransaction(pf)
		}()

		orm.SelectOne(&objA, "SELECT * FROM test_orm_a123 WHERE test_id = ?", testObj.TestID)
		if objA.Description != "test" {
			t.Fatal("incorrect description")
		}
		orm.SelectOne(&objB, "SELECT * FROM test_orm_b999 WHERE test_id = ?", testObjB.TestID)
		if objB.Description != "b" {
			t.Fatal("incorrect description")
		}

		ef := func(o *ORMTran) error {
			o.Exec("update test_orm_a123 set description = 'test2'")
			_, err := o.Exec("update test_orm_b999 set description1 = 'b2'") // will return err here
			return err
		}
		err := orm.DoTransaction(ef)
		t.Logf("expected error here: %v", err)
		if err == nil {
			t.Fatal("should error")
		}

		orm.SelectOne(&objA, "SELECT * FROM test_orm_a123 WHERE test_id = ?", testObj.TestID)
		if objA.Description != "test" {
			t.Fatal("incorrect description")
		}
		orm.SelectOne(&objB, "SELECT * FROM test_orm_b999 WHERE test_id = ?", testObjB.TestID)
		if objB.Description != "b" {
			t.Fatal("incorrect description")
		}

		f = func(o *ORMTran) error {
			o.Exec("update test_orm_a123 set description = 'test3'")
			o.Exec("update test_orm_b999 set description = 'b3'")
			return nil
		}
		orm.DoTransaction(f)

		orm.SelectOne(&objA, "SELECT * FROM test_orm_a123 WHERE test_id = ?", testObj.TestID)
		if objA.Description != "test3" {
			t.Fatal("incorrect description")
		}
		orm.SelectOne(&objB, "SELECT * FROM test_orm_b999 WHERE test_id = ?", testObjB.TestID)
		if objB.Description != "b3" {
			t.Fatal("incorrect description")
		}
	})
}

func TestORM_SelectRawSet(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		testObj := &TestOrmE333{
			Name:        "name",
			VInt64:      -64,
			VInt:        3,
			VUint64:     64,
			VUint:       3,
			VBoolean:    true,
			VBigDecimal: 3.131495276,
			VFloat:      3.131495276,
		}

		columnsMap := map[string]string{
			"test_orm_e_id": "int64",
			"name":          "string",
			"description":   "string",
			"v_int64":       "int64",
			"v_int":         "int",
			"v_uint64":      "uint64",
			"v_uint":        "uint",
			"v_boolean":     "boolean",
			"v_big_decimal": "float64",
			"v_float":       "float64",
		}

		err := orm.Insert(testObj)
		if err != nil {
			t.Errorf("got error while insert err=%+v", err)
		}

		results, err := orm.SelectRawSet("select * from test_orm_e333", columnsMap)

		if err != nil {
			t.Errorf("got error while select err=%+v", err)
		}

		t.Logf("results = %+v\n", results)

		if len(results) != 1 {
			t.Errorf("results should be 1")
		}

		if results[0]["v_int64"] != int64(-64) {
			t.Errorf("got error, v_int64 value is not correct, v_int64=%d", results[0]["v_int64"])
		}

	})
}

func TestORM_SelectRawSetWithParam(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		testObj := &TestOrmE333{
			Name:        "name",
			VInt64:      -64,
			VInt:        3,
			VUint64:     64,
			VUint:       3,
			VBoolean:    true,
			VBigDecimal: 3.131495276,
			VFloat:      3.131495276,
		}

		err := orm.Insert(testObj)
		if err != nil {
			t.Errorf("got error while insert err=%+v", err)
		}

		results, err := orm.SelectRawSetWithParam("select * from test_orm_e333 where name = #{name}", map[string]interface{}{
			"name": "name",
		})

		if err != nil {
			t.Errorf("got error while select err=%+v", err)
		}

		t.Logf("results = %+v\n", results)

		if len(results) != 1 {
			t.Errorf("results should be 1")
		}

		if results[0]["v_int64"] != int64(-64) {
			t.Errorf("got error, v_int64 value is not correct, v_int64=%d", results[0]["v_int64"])
		}

	})
}

/*
不获取id
insert 1000000 records cost time  3.589199155s
insert 1000000 records cost time  3.59200054s
insert 1000000 records cost time  3.555736898s
insert 1000000 records cost time  3.605326779s
insert 1000000 records cost time  3.691365914s
insert 1000000 records cost time  3.646872053s

获取id
insert 1000000 records cost time  3.466648204s
insert 1000000 records cost time  3.467752121s
insert 1000000 records cost time  3.502699721s
insert 1000000 records cost time  3.534404949s
insert 1000000 records cost time  3.458686129s
insert 1000000 records cost time  3.460533604s
insert 1000000 records cost time  3.573200456s
insert 1000000 records cost time  3.542086779s
insert 1000000 records cost time  3.41414727s
insert 1000000 records cost time  3.503617567s
insert 1000000 records cost time  3.696351458s
insert 1000000 records cost time  3.469509861s
*/
func TestInsertBatch(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		list := make([]interface{}, 0, 1000000)
		for i := 0; i < 1000000; i++ {
			list = append(list, &TestOrmA123{
				OtherId:     1,
				TestOrmDId:  0,
				Description: "test orm 1测试",
				StartDate:   time.Now(),
			})
		}
		start := time.Now()
		orm.InsertBatch(list)
		fmt.Println("insert 1000000 records cost time ", time.Now().Sub(start))
	})
}

func TestTableName(t *testing.T) {
	oneTestScope(func(orm *ORM, testTableName string) {
		obj := TestOrmF123{
			Name: "tf1",
		}
		err := orm.Insert(&obj)
		if err != nil {
			t.Fatal("should not return error when inserting obj")
		}

		if obj.Id <= 0 {
			t.Fatal("id should be larger than 0 after insertion")
		}

		var obj1 TestOrmF123
		err = orm.SelectByPK(&obj1, obj.Id)
		if err != nil {
			t.Fatal("should not return error when selecting")
		}
		if obj.Name != obj1.Name {
			t.Fatal("obj name doesn't match")
		}

		list := make([]interface{}, 0, 10000)
		for i := 0; i < 20000; i++ {
			list = append(list, &TestOrmF123{
				Name: "test",
			})
		}
		err = orm.InsertBatch(list)
		if err != nil {
			t.Fatal("got error when batch inserting", err)
		}

		result := make([]int64, 0)
		err = orm.Select(&result, "SELECT id FROM orm_f")
		if err != nil {
			t.Fatal("select int array error", err)
		}
		t.Log(result)
	})
}
