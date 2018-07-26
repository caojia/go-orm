/*
MySQL的ORM框架，主要包含了通过反射将sql的Result映射成结构.
*/
package orm

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

var sqlParamReg *regexp.Regexp
var initOnce sync.Once
var sqlLogger SqlLogger = &VerboseSqlLogger{}

func SetLog(sqlLog SqlLogger) {
	sqlLogger = sqlLog
}

/**
把数据库中的字段(可以处理带下划线的字段)转化为struct中的字段，首字母大写和驼峰
*/
func colName2FieldName(buf string) string {
	tks := strings.Split(buf, "_")
	ret := ""
	for _, tk := range tks {
		ret += strings.Title(tk)
	}
	return ret
}

/**
把struct中的字段转化为数据库中的字段
*/
func fieldName2ColName(buf string) string {
	w := bytes.Buffer{}
	for k, c := range buf {
		if unicode.IsUpper(c) {
			if k > 0 {
				w.WriteString("_")
			}
			w.WriteRune(unicode.ToLower(c))
		} else {
			w.WriteRune(c)
		}
	}
	return w.String()
}

//获取db标签中的内容，如果db标签为空，对json标签进行兼容
func getDbTagMap(t reflect.Type, n int) map[string]string {
	dbTags := make(map[string]string, n)
	for k := 0; k < t.NumField(); k++ {
		ft := t.Field(k)
		dbTag := ft.Tag.Get("db")
		dbCol := getDbTagCol(dbTag)
		if dbCol != "" { //获取struct中的db标签组成字典，在后面更新中，只要字典存在的值，就进行替换
			dbTags[dbCol] = ft.Name
		} else {
			jsonTag := ft.Tag.Get("json")
			if jsonTag != "" {
				dbTags[strings.Split(jsonTag, ",")[0]] = ft.Name
			}
		}
	}
	return dbTags
}

/*
 通过reflect把row中的值映射到一个struct中
*/
func reflectStruct(s interface{}, cols []string, row *sql.Rows) error {
	v := reflect.ValueOf(s)
	t := reflect.TypeOf(s)
	return reflectStructValue(v, t.Elem(), cols, row)
}
func reflectStructValue(v reflect.Value, t reflect.Type, cols []string, row *sql.Rows) error {
	if v.Kind() != reflect.Ptr {
		return errors.New("holder should be pointer")
	}
	v = v.Elem()
	targets := make([]interface{}, len(cols))
	//修改映射关系,建立db的对应关系
	dbTags := getDbTagMap(t, len(cols))

	for k, c := range cols {
		col := ""
		if temp, ok := dbTags[c]; ok { //先进行字典匹配
			col = temp
		} else {
			col = colName2FieldName(c)
		}
		fv := v.FieldByName(col)
		if !fv.CanAddr() {
			logrus.Infof("missing filed :%s", c)
			var b interface{}
			targets[k] = &b
		} else {
			targets[k] = fv.Addr().Interface()
		}
	}
	return row.Scan(targets...)
}

func checkStruct(s interface{}, cols []string, tableName string) error {

	v := reflect.TypeOf(s)

	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	for _, c := range cols {
		_, ok := v.FieldByName(colName2FieldName(c))
		if !ok {
			return errors.New(tableName + " missing field " + c)
		}
	}
	return nil
}

type Tdx interface {
	Exec(string, ...interface{}) (sql.Result, error)
	Query(string, ...interface{}) (*sql.Rows, error)
}

func getColumns(tdx Tdx, tableName string) ([]string, error) {
	ret := []string{}
	rows, err := tdx.Query("show columns from " + tableName)
	if err != nil {
		return ret, err
	}
	defer rows.Close()
	for rows.Next() {
		var name, tp, nu, key, dft, extra sql.NullString
		if err := rows.Scan(&name, &tp, &nu, &key, &dft, &extra); err != nil {
			return ret, errors.New("can not scan filed:" + err.Error())
		}
		ret = append(ret, name.String)
	}
	if err := rows.Err(); err != nil {
		return ret, err
	}
	return ret, nil
}

func checkTableColumns(tdx Tdx, s interface{}) error {
	tableName := getTableName(s)
	cols, err := getColumns(tdx, tableName)
	if err != nil {
		return err
	}
	logrus.WithField("table", tableName).WithField("cols", cols).Info()
	return checkStruct(s, cols, tableName)
}

type SqlLog struct {
	Sql      string        `json:"sql"`
	Duration time.Duration `json:"duration"`
	Explain  []*Explain    `json:"explain,omitempty"`
}
type Explain struct {
	Table  string `json:"table,omitempty"`
	Type   string `json:"type,omitempty"`
	Key    string `json:"key,omitempty"`
	KeyLen int64  `json:"key_len,omitempty"`
	Ref    string `json:"ref,omitempty"`
	Rows   int64  `json:"rows,omitempty"`
	Extra  string `json:"extra,omitempty"`
}
type SqlLogger interface {
	Log(c context.Context, sqlLog *SqlLog)
	ShowExplain(dur time.Duration) bool
}
type VerboseSqlLogger struct{}

func (n *VerboseSqlLogger) Log(c context.Context, sqlLog *SqlLog) {
	logs := logrus.WithFields(logrus.Fields{
		"Sql":      sqlLog.Sql,
		"Duration": sqlLog.Duration,
	})
	if len(sqlLog.Explain) > 0 {
		data, _ := json.Marshal(sqlLog.Explain)
		logs = logs.WithField("Explain", string(data))
	}
	logs.Info()
}

func (n *VerboseSqlLogger) ShowExplain(dur time.Duration) bool {
	return dur >= 200*time.Millisecond
}

func logPrint(c context.Context, logger SqlLogger, exp []*Explain, duration time.Duration, queryStr string, args ...interface{}) {
	queryStr = regexp.MustCompile("\\s+").ReplaceAllString(queryStr, " ")
	newArgs := make([]interface{}, 0)
	for _, arg := range args {
		t := reflect.TypeOf(arg)
		switch t.Kind() {
		case reflect.Ptr:
			v := reflect.ValueOf(arg)
			if t.String() == "*time.Time" { //对时间进行处理
				newArgs = append(newArgs, arg.(*time.Time).Format("2006-01-02 15:04:05"))
			} else {
				newArgs = append(newArgs, v.Elem().Interface())
			}
		default:
			newArgs = append(newArgs, arg)
		}
	}
	sqlLog := SqlLog{Duration: duration, Sql: fmt.Sprintf("%s%+v", queryStr, newArgs), Explain: exp}
	logger.Log(c, &sqlLog)
}
func doExplain(tdx Tdx, query string, args ...interface{}) ([]*Explain, error) {
	explainStr := fmt.Sprintf("explain %s", query)
	type explain struct {
		Id           sql.NullInt64  `json:"id"`
		SelectType   sql.NullString `json:"select_type"`
		Partitions   sql.NullString `json:"partitions"`
		PossibleKeys sql.NullString `json:"possible_keys"`
		KeyLen       sql.NullInt64  `json:"key_len"`
		Filtered     sql.NullString `json:"filtered"`
		Table        sql.NullString `json:"table"`
		Type         sql.NullString `json:"type"`
		Key          sql.NullString `json:"key"`
		Ref          sql.NullString `json:"ref"`
		Rows         sql.NullInt64  `json:"rows"`
		Extra        sql.NullString `json:"extra"`
	}
	rows, err := tdx.Query(explainStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var exp []*Explain
	for rows.Next() {
		e := explain{}
		cols, err := rows.Columns()
		err = reflectStruct(&e, cols, rows)
		if err != nil {
			logrus.WithError(err).Error("reflect err")
		}
		exp = append(exp, &Explain{Table: e.Table.String, KeyLen: e.KeyLen.Int64, Type: e.Type.String, Key: e.Key.String, Ref: e.Ref.String, Rows: e.Rows.Int64, Extra: e.Extra.String})
	}
	return exp, nil
}
func exec(c context.Context, tdx Tdx, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	query, args = changeSQLIn(query, args...)
	res, err := tdx.Exec(query, args...)
	if err != nil { //更换处理方式，如果是err就直接打印err日志，不打印其他日志，不用多执行一遍exec
		return res, err
	}
	logPrint(c, sqlLogger, nil, time.Since(start), query, args...)
	return res, err
}

func query(c context.Context, tdx Tdx, queryStr string, args ...interface{}) (res *sql.Rows, err error) {
	queryStr = addLimit(queryStr, 0)
	queryStr, args = changeSQLIn(queryStr, args...)
	start := time.Now()
	if res, err = tdx.Query(queryStr, args...); err != nil {
		return res, err
	}
	duration := time.Since(start)

	var exp []*Explain
	if sqlLogger.ShowExplain(duration) {
		exp, err = doExplain(tdx, queryStr, args...)
		if err != nil {
			return nil, err
		}
	}
	logPrint(c, sqlLogger, exp, duration, queryStr, args...)
	return res, nil
}

func execWithParam(c context.Context, tdx Tdx, paramQuery string, paramMap interface{}) (sql.Result, error) {
	params := sqlParamReg.FindAllString(paramQuery, -1)
	if params != nil && len(params) > 0 {
		var args []interface{} = make([]interface{}, 0, len(params))
		for _, param := range params {
			param = param[2 : len(param)-1]
			value, err := getFieldValue(paramMap, param)
			if err != nil {
				return nil, err
			}
			args = append(args, value)
		}
		paramQuery = sqlParamReg.ReplaceAllLiteralString(paramQuery, "?")
		return exec(c, tdx, paramQuery, args...)
	} else {
		return exec(c, tdx, paramQuery)
	}
}

func execWithRowAffectCheck(c context.Context, tdx Tdx, expectRows int64, query string, args ...interface{}) error {
	ret, err := exec(c, tdx, query, args...)
	if err != nil {
		return err
	}
	ra, err := ret.RowsAffected()
	if err != nil {
		return err
	}
	if ra != expectRows {
		return errors.New(fmt.Sprintf("[RowAffectCheckError]: query [%s] should only affect %d rows, really affect %d rows", query, expectRows, ra))
	}
	return nil
}

func getPKColumn(s interface{}) string {
	t := reflect.TypeOf(s).Elem()
	return getPkColumnByType(t)
}

//获取标签为pk的col
func getPkColumnByType(t reflect.Type) string {
	for k := 0; k < t.NumField(); k++ {
		ft := t.Field(k)
		dbTag := ft.Tag.Get("db")
		dbCol := getDbTagCol(dbTag)
		if dbCol == "" { //兼容json标签
			jsonTag := ft.Tag.Get("json")
			if jsonTag != "" {
				dbCol = strings.Split(jsonTag, ",")[0]
			}
		}
		if ft.Tag.Get("pk") == "true" || isPkOrAi(dbTag, "pk") {
			if dbCol != "" {
				return dbCol
			}
			return fieldName2ColName(ft.Name)
		}
	}
	return ""
}

type orColumn struct {
	fieldName string
	or        string
	table     string
	orType    reflect.Type
}

/**
返回三个值，一个是struct主键的field，一个是主键对应的数据库的值 ，一个是[]*orColumn
*/
func getOrColumns(s interface{}) (string, string, []*orColumn, error) {
	t := reflect.TypeOf(s).Elem()
	return getOrColumnsByType(t)
}

/**
根据struct中的值找到其他struct进行relation的联系，并组成[]*orColumn结构,返回三个值，一个是struct主键的field，一个是主键对应的数据库的值 ，一个是[]*orColumn
*/
func getOrColumnsByType(t reflect.Type) (string, string, []*orColumn, error) {
	res := make([]*orColumn, 0)
	pkCol := ""
	pkField := ""
	// TODO: error check, i.e., has_one field must be a pointer of registered model
	for k := 0; k < t.NumField(); k++ {
		ft := t.Field(k)
		orTag := ft.Tag.Get("or")
		if orTag != "" {
			if orTag == "has_one" || orTag == "has_many" || orTag == "belongs_to" {
				var orType reflect.Type
				if orTag == "has_one" {
					if ft.Type.Kind() != reflect.Ptr {
						return "", "", res, errors.New(ft.Name + " should be pointer")
					}
					orType = ft.Type.Elem()
				} else if orTag == "has_many" {
					if ft.Type.Kind() != reflect.Slice {
						return "", "", res, errors.New(ft.Name + " should be slice of pointer")
					}
					elemType := ft.Type.Elem()
					if elemType.Kind() != reflect.Ptr {
						return "", "", res, errors.New(ft.Name + " should be slice of pointer")
					}
					orType = elemType.Elem()
				} else if orTag == "belongs_to" {
					if ft.Type.Kind() != reflect.Ptr {
						return "", "", res, errors.New(ft.Name + " should be pointer")
					}
					orType = ft.Type.Elem()
				}
				orTableName := ft.Tag.Get("table")
				if orTableName == "" {
					return "", "", res, errors.New("invalid table name in or tag on field: " + ft.Name)
				}
				res = append(res, &orColumn{
					fieldName: ft.Name,
					or:        orTag,
					table:     orTableName,
					orType:    orType,
				})
			} else {
				return "", "", res, errors.New("unsupported or tag: " + orTag + ", only support has_one, has_many and belongs_to for now")
			}
		}
		dbTag := ft.Tag.Get("db")
		dbCol := getDbTagCol(dbTag)
		if dbCol == "" { //兼容json标签
			jsonTag := ft.Tag.Get("json")
			if jsonTag != "" {
				dbCol = strings.Split(jsonTag, ",")[0]
			}
		}
		if ft.Tag.Get("pk") == "true" || isPkOrAi(dbTag, "pk") {
			pkCol = dbCol
			if pkCol == "" {
				pkCol = fieldName2ColName(ft.Name)
			}
			pkField = ft.Name
		}
	}
	return pkField, pkCol, res, nil
}

/**
通过struct中实现的TableName方法来反射调用获取表的表名
*/
func getTableName(s interface{}) string {
	tableNameMethod := reflect.ValueOf(s).MethodByName("TableName")
	if tableNameMethod.IsValid() {
		tableName := tableNameMethod.Call([]reflect.Value{})
		if len(tableName) == 1 && tableName[0].Kind() == reflect.String {
			return tableName[0].String()
		}
	}
	ts := reflect.TypeOf(s)
	if ts.Kind() == reflect.Ptr {
		ts = ts.Elem()
	}
	return fieldName2ColName(ts.Name())
}

func selectByPK(c context.Context, tdx Tdx, s interface{}, pk interface{}) error {
	pkName := getPKColumn(s)
	tabName := getTableName(s)
	if pkName == "" {
		return errors.New(tabName + " does not have primary key")
	}
	return selectOne(c, tdx, s, fmt.Sprintf("select * from %s where %s = ?", tabName, pkName), pk)
}

func selectOne(c context.Context, tdx Tdx, s interface{}, query string, args ...interface{}) error {
	query = addLimit(query, 1)
	// One time there only can be one active sql Rows query
	err := selectOneInternal(c, tdx, s, query, args...)
	if err != nil {
		return err
	}
	pkField, pkCol, orColumns, err := getOrColumns(s)
	if err != nil {
		return err
	}
	if orColumns != nil && len(orColumns) > 0 {
		v := reflect.ValueOf(s).Elem()
		pkValue, err := getFieldValue(s, pkField)
		if err != nil {
			return err
		}
		for _, orCol := range orColumns {
			if orCol.or == "has_one" {
				err = processOrHasOneRelation(c, tdx, orCol, v, pkCol, pkValue)
				if err != nil {
					return err
				}
			} else if orCol.or == "has_many" {
				orField := v.FieldByName(orCol.fieldName)
				err = selectManyInternal(c, tdx, orField.Addr().Interface(), false,
					"SELECT * FROM "+orCol.table+" WHERE "+pkCol+" = ?", pkValue)
				if err != nil {
					return err
				}
			} else if orCol.or == "belongs_to" {
				fk := getPkColumnByType(orCol.orType)
				if fk == "" {
					return errors.New("error while getting primary key of " + orCol.table + " for belongs_to")
				}
				fkValue, err := getFieldValue(s, colName2FieldName(fk))
				if err != nil {
					return err
				}
				err = processOrBelongsToRelation(c, tdx, orCol, v, fk, fkValue)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func selectOneInternal(c context.Context, tdx Tdx, s interface{}, queryStr string, args ...interface{}) error {
	rows, err := query(c, tdx, queryStr, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return sql.ErrNoRows
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	err = reflectStruct(s, cols, rows)
	if err != nil {
		return err
	}

	return nil
}

func processOrHasOneRelation(c context.Context, tdx Tdx, orCol *orColumn, v reflect.Value, pkCol string, pkValue interface{}) error {
	queryStr := fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` = ? LIMIT 1", orCol.table, pkCol)
	rows, err := query(c, tdx, queryStr, pkValue)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	orField := v.FieldByName(orCol.fieldName)
	orValue := reflect.New(orField.Type().Elem())
	err = reflectStructValue(orValue, orField.Type().Elem(), cols, rows)
	if err != nil {
		return err
	}
	orField.Set(orValue)
	return nil
}

func processOrBelongsToRelation(c context.Context, tdx Tdx, orCol *orColumn, v reflect.Value, fk string, fkValue interface{}) error {
	queryStr := fmt.Sprintf("SELECT * FROM %s WHERE %s = ? LIMIT 1", orCol.table, fk)
	orRows, err := query(c, tdx, queryStr, fkValue)
	if err != nil {
		return err
	}
	defer orRows.Close()

	if !orRows.Next() {
		return nil
	}
	orCols, err := orRows.Columns()
	if err != nil {
		return err
	}
	orField := v.FieldByName(orCol.fieldName)
	orValue := reflect.New(orField.Type().Elem())
	err = reflectStructValue(orValue, orField.Type().Elem(), orCols, orRows)

	if err != nil {
		return err
	}
	orField.Set(orValue)
	return nil
}

func selectStr(c context.Context, tdx Tdx, queryStr string, args ...interface{}) (string, error) {
	queryStr = addLimit(queryStr, 1)
	rows, err := query(c, tdx, queryStr, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if !rows.Next() {
		return "", sql.ErrNoRows
	}
	ret := ""
	err = rows.Scan(&ret)
	return ret, err
}

func selectInt(c context.Context, tdx Tdx, queryStr string, args ...interface{}) (int64, error) {
	queryStr = addLimit(queryStr, 1)
	rows, err := query(c, tdx, queryStr, args...)
	var ret int64
	if err != nil {
		return ret, err
	}
	defer rows.Close()

	if !rows.Next() {
		return ret, sql.ErrNoRows
	}

	err = rows.Scan(&ret)
	return ret, err
}

//判断是否为 slice的指针类型
func toSliceType(i interface{}) (reflect.Type, error) {
	t := reflect.TypeOf(i)
	//判断是否数组
	if t.Kind() != reflect.Ptr {
		if t.Kind() == reflect.Slice {
			return nil, errors.New("can not select into a non-pointer slice")
		}
		return nil, nil
	}
	//判断该指针是否为数组类型的指针
	if t = t.Elem(); t.Kind() != reflect.Slice {
		return nil, errors.New("can not select into a non-pointer slice")
	}
	return t.Elem(), nil
}

/*
func MapScan(r ColScanner, dest map[string]interface{}) error {
	// ignore r.started, since we needn't use reflect for anything.
	columns, err := r.Columns()
	if err != nil {
		return err
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}

	err = r.Scan(values...)
	if err != nil {
		return err
	}

	for i, column := range columns {
		dest[column] = *(values[i].(*interface{}))
	}

	return r.Err()
}*/

func selectRawSet(c context.Context, tdx Tdx, queryStr string, columnMaps map[string]string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := query(c, tdx, queryStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dataSet := make([]map[string]interface{}, 0, 1)

	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			return dataSet, err
		}
		itemMap := make(map[string]interface{})
		itemList := make([]interface{}, len(cols))
		for i := range itemList {
			itemList[i] = new(interface{})
		}

		err = rows.Scan(itemList...)

		if err != nil {
			logrus.WithError(err).WithField("rows", rows).Error("scan err")
			return dataSet, err
		}
		for k, c := range cols {
			columnType, ok := columnMaps[c]
			item := *itemList[k].(*interface{})
			if ok {
				itemMap[c], err = NormalizeValue(columnType, item)
				if err != nil {
					itemMap[c] = item
				}
			} else {
				itemMap[c] = item
			}
		}
		dataSet = append(dataSet, itemMap)
	}
	return dataSet, nil
}

func selectRaw(c context.Context, tdx Tdx, queryStr string, args ...interface{}) ([]string, [][]interface{}, error) {

	rows, err := query(c, tdx, queryStr, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	data := [][]interface{}{}
	if err != nil {
		return colNames, data, err
	}

	for rows.Next() {
		itemMap := make([]interface{}, len(colNames))
		itemList := make([]interface{}, len(colNames))
		for i := range itemList {
			itemList[i] = new(interface{})
		}

		err = rows.Scan(itemList...)

		if err != nil {
			logrus.WithError(err).WithField("rows", rows).Error("scan err")
			return colNames, data, err
		}
		for k, _ := range colNames {
			switch t := (*itemList[k].(*interface{})).(type) {
			case []uint8:
				itemMap[k] = string(t[:])
			case time.Time:
				itemMap[k] = t.Format("2006-01-02 15:04:05")
			case int64:
				itemMap[k] = t
			case int32:
				itemMap[k] = t
			case int:
				itemMap[k] = t
			case float32:
				itemMap[k] = t
			case float64:
				itemMap[k] = t
			case string:
				itemMap[k] = t
			case nil:
				itemMap[k] = nil
			default:
			}
		}
		data = append(data, itemMap)
	}
	return colNames, data, nil
}

func selectRawWithParam(c context.Context, tdx Tdx, paramQuery string, paramMap interface{}) ([]string, [][]interface{}, error) {
	params := sqlParamReg.FindAllString(paramQuery, -1)
	if params != nil && len(params) > 0 {
		var args []interface{} = make([]interface{}, 0, len(params))
		for _, param := range params {
			param = param[2 : len(param)-1]
			value, err := getFieldValue(paramMap, param)
			if err != nil {
				return nil, nil, err
			}
			args = append(args, value)
		}
		paramQuery = sqlParamReg.ReplaceAllLiteralString(paramQuery, "?")
		return selectRaw(c, tdx, paramQuery, args...)
	} else {
		return selectRaw(c, tdx, paramQuery)
	}
}

func selectRawSetWithParam(c context.Context, tdx Tdx, paramQuery string, paramMap interface{}) ([]map[string]interface{}, error) {
	headers, rows, err := selectRawWithParam(c, tdx, paramQuery, paramMap)
	if err != nil {
		return nil, err
	}
	results := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		results[i] = map[string]interface{}{}
		for j, h := range headers {
			results[i][h] = row[j]
		}
	}
	return results, nil
}

func selectMany(c context.Context, tdx Tdx, s interface{}, query string, args ...interface{}) error {
	query = addLimit(query, 0)
	return selectManyInternal(c, tdx, s, true, query, args...)
}

//搜索一个数组，并支持关联，当搜索单数组
func selectManyInternal(c context.Context, tdx Tdx, s interface{}, processOr bool, queryStr string, args ...interface{}) error {
	t, err := toSliceType(s)
	if err != nil {
		return err
	}

	if t.Kind() != reflect.Ptr && t.Kind() != reflect.Int64 && t.Kind() != reflect.String &&
		t.Kind() != reflect.Int && t.Kind() != reflect.Int32 && t.Kind() != reflect.Bool && t.Kind() != reflect.Float64 &&
		t.Kind() != reflect.Float32 && t.Kind() != reflect.Uint64 && t.Kind() != reflect.Uint {
		return errors.New("slice elements type " + t.Kind().String() + " not supported")
	}

	var isPtr = t.Kind() == reflect.Ptr

	hasOrCols := false
	pkCol := ""
	pkField := ""
	var orCols []*orColumn = nil
	if isPtr {
		t = t.Elem()
		if processOr {
			pkField, pkCol, orCols, err = getOrColumnsByType(t)
			if err != nil {
				return err
			}
			if orCols != nil && len(orCols) > 0 {
				hasOrCols = true
			}
		}
	}
	//进行查询
	sliceValue := reflect.Indirect(reflect.ValueOf(s))
	rows, err := query(c, tdx, queryStr, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	keys := make([]interface{}, 0)
	resMap := map[interface{}]reflect.Value{}
	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		v := reflect.New(t)
		if isPtr {
			targets := make([]interface{}, len(cols))
			//修改映射关系
			dbTags := getDbTagMap(t, len(cols))
			for k, c := range cols {
				fName := ""
				if temp, ok := dbTags[c]; ok {
					fName = temp
				} else {
					fName = colName2FieldName(c)
				}
				fv := v.Elem().FieldByName(fName)
				if !fv.CanAddr() {
					logrus.WithField("sql", queryStr).Errorf("missing field: %s", fName)
					var b interface{}
					targets[k] = &b
				} else {
					targets[k] = fv.Addr().Interface()
				}
			}
			err = rows.Scan(targets...)
			if err != nil {
				return err
			}
			sliceValue.Set(reflect.Append(sliceValue, v))
			if hasOrCols {
				pkFv := v.Elem().FieldByName(pkField)
				if pkFv.IsValid() {
					key := pkFv.Interface()
					keys = append(keys, key)
					resMap[key] = v
				}
			}
		} else {
			err = rows.Scan(v.Interface())
			if err != nil {
				return err
			}
			sliceValue.Set(reflect.Append(sliceValue, v.Elem()))
		}
	}
	if len(keys) > 0 {
		for _, orCol := range orCols {
			var sqlQuery string
			// 如果是belongs_to，需要先把fk -> array(elem)存下来，然后根据数据库请求结果将对应fk的指针指向相应的关联对象
			if orCol.or == "belongs_to" {
				fk := getPkColumnByType(orCol.orType)
				if fk == "" {
					return errors.New("error while getting primary key of " + orCol.table + " for belongs_to")
				}
				fkCol := colName2FieldName(fk)
				fkValues := make([]interface{}, 0)
				fkMaps := map[interface{}][]reflect.Value{}
				i := 0
				for _, value := range resMap {
					fkValue, err := getFieldValue(value.Interface(), fkCol)
					if err != nil {
						return err
					}
					fkValues = append(fkValues, fkValue)
					if v, ok := fkMaps[fkValue]; ok {
						fkMaps[fkValue] = append(v, value)
					} else {
						fkMaps[fkValue] = make([]reflect.Value, 0)
						fkMaps[fkValue] = append(fkMaps[fkValue], value)
					}
					i = i + 1
				}
				sqlQuery = makeString("SELECT * FROM "+orCol.table+" WHERE "+fk+" in (", ",", ")", fkValues)
				orRows, err := query(c, tdx, sqlQuery)

				if err != nil {
					return err
				}
				defer orRows.Close()
				for orRows.Next() {
					orCols, err := orRows.Columns()
					if err != nil {
						return err
					}
					orValue := reflect.New(orCol.orType)
					err = reflectStructValue(orValue, orCol.orType, orCols, orRows)
					if err != nil {
						return err
					}
					keyValue := orValue.Elem().FieldByName(fkCol)
					if keyValue.IsValid() {
						if arr, ok := fkMaps[keyValue.Interface()]; ok {
							for _, v := range arr {
								v.Elem().FieldByName(orCol.fieldName).Set(orValue)
							}
						}

					}
				}
			} else {
				sqlQuery = makeString("SELECT * FROM "+orCol.table+" WHERE "+pkCol+" in (",
					",", ")", keys)
				orRows, err := query(c, tdx, sqlQuery)

				if err != nil {
					return err
				}
				defer orRows.Close()
				for orRows.Next() {
					orCols, err := orRows.Columns()
					if err != nil {
						return err
					}
					orValue := reflect.New(orCol.orType)
					err = reflectStructValue(orValue, orCol.orType, orCols, orRows)
					if err != nil {
						return err
					}
					keyValue := orValue.Elem().FieldByName(pkField)
					if keyValue.IsValid() {
						if v, ok := resMap[keyValue.Interface()]; ok {
							if orCol.or == "has_one" {
								v.Elem().FieldByName(orCol.fieldName).Set(orValue)
							} else if orCol.or == "has_many" {
								orSliceValue := v.Elem().FieldByName(orCol.fieldName)
								orSliceValue.Set(reflect.Append(orSliceValue, orValue))
							}
						}
					}
				}
			}
		}
	}
	return nil
}

func makeString(start, split, end string, ids []interface{}) string {
	buff := bytes.Buffer{}
	buff.WriteString(start)
	len := len(ids)
	for i, v := range ids {
		buff.WriteString(fmt.Sprintf("%v", v))
		if i < len-1 {
			buff.WriteString(split)
		}
	}
	buff.WriteString(end)
	return buff.String()
}

var zeroTime = time.Unix(1, 0)

//通过fields中的字段获取部分数据以及主建和主键的值
func columnsByStructFields(s interface{}, cols []string) ([]interface{}, reflect.Value, bool, string) {
	t := reflect.TypeOf(s).Elem()
	v := reflect.ValueOf(s).Elem()
	ret := make([]interface{}, 0, len(cols))
	var pk reflect.Value
	var pkName string
	isAi := false
	//反射遍历整个struct找到主键和主键的值，一般都是第一个
	for k := 0; k < t.NumField(); k++ {
		ft := t.Field(k)
		//先匹配db中的pk
		dbTag := ft.Tag.Get("db")
		dbCol := getDbTagCol(dbTag)
		jsonTag := strings.Split(ft.Tag.Get("json"), ",")[0]
		if ft.Tag.Get("pk") == "true" || isPkOrAi(dbTag, "pk") {
			pk = v.Field(k)
			pkName = dbCol
			if jsonTag != "" && pkName == "" {
				pkName = jsonTag
			}
			if pkName == "" {
				pkName = fieldName2ColName(ft.Name)
			}
			if ft.Tag.Get("ai") == "true" || isPkOrAi(dbTag, "ai") {
				isAi = true
			}
			break
		}
	}
	//通过cols获取struct中的值
	for _, value := range cols {
		value = colName2FieldName(value)
		r := v.FieldByName(value).Addr().Interface()
		if v.FieldByName(value).Type().String() == "time.Time" {
			if r.(*time.Time).IsZero() {
				r = &zeroTime
			}
		}
		ret = append(ret, r)
	}
	return ret, pk, isAi, pkName
}

/**
解析一个struct，解析适合数据库操作的cols，vals,args，还有主键和主键值
*/
func columnsByStruct(s interface{}) (string, string, []interface{}, reflect.Value, bool, string) {
	t := reflect.TypeOf(s).Elem()
	v := reflect.ValueOf(s).Elem()
	cols := ""
	vals := ""
	ret := make([]interface{}, 0, t.NumField())
	n := 0
	var pk reflect.Value
	var pkName string
	isAi := false
	for k := 0; k < t.NumField(); k++ {
		ft := t.Field(k)
		//优先对标签进行处理，当没有找到标签时，就直接对struct的字段进行转化

		dbTag := ft.Tag.Get("db")
		str := getDbTagCol(dbTag)
		if str == "" {
			str = strings.Split(ft.Tag.Get("json"), ",")[0]
			if str == "" {
				str = fieldName2ColName(ft.Name)
			}
		}
		//auto increment field
		if ft.Tag.Get("pk") == "true" || isPkOrAi(dbTag, "pk") {
			pk = v.Field(k)
			pkName = str
			if pkName == "" {
				pkName = fieldName2ColName(ft.Name)
			}
			if ft.Tag.Get("ai") == "true" || isPkOrAi(dbTag, "ai") {
				isAi = true
				continue
			}
		}

		//auto update filed, created_at, updated_at, etc.
		if ft.Tag.Get("ignore") == "true" || ft.Tag.Get("or") != "" {
			continue
		}

		if n > 0 {
			cols += ","
			vals += ","
		}
		cols += str
		vals += "?"
		r := v.Field(k).Addr().Interface()
		if v.Field(k).Type().String() == "time.Time" {
			if r.(*time.Time).IsZero() {
				r = &zeroTime
			}
		}

		ret = append(ret, r)
		n += 1
	}
	return cols, vals, ret, pk, isAi, pkName
}

func columnsBySlice(s []interface{}) (string, string, []interface{}, []reflect.Value, []bool) {
	t := reflect.TypeOf(s[0]).Elem()
	ret := make([]interface{}, 0, t.NumField()*len(s))
	cols := "("
	isFirst := true
	for k := 0; k < t.NumField(); k++ {
		ft := t.Field(k)
		dbTag := ft.Tag.Get("db")
		str := getDbTagCol(dbTag)
		if str == "" {
			str = fieldName2ColName(ft.Name)
		}
		if ft.Tag.Get("pk") == "true" || isPkOrAi(dbTag, "pk") {
			if ft.Tag.Get("ai") == "true" || isPkOrAi(dbTag, "ai") {
				continue
			}
		}
		if ft.Tag.Get("ignore") == "true" || ft.Tag.Get("or") != "" {
			continue
		}
		if !isFirst {
			cols += ","
		}
		cols += str
		isFirst = false
	}
	cols += ")"

	vals := bytes.Buffer{}
	pks := make([]reflect.Value, len(s))
	ais := make([]bool, len(s))
	for n, record := range s {
		ct := reflect.TypeOf(record).Elem()
		if ct.Name() != t.Name() {
			continue
		}
		v := reflect.ValueOf(record).Elem()
		if n > 0 {
			vals.WriteString(",")
		}
		vals.WriteString("(")
		isFirst := true
		for k := 0; k < t.NumField(); k++ {
			ft := t.Field(k)
			dbTag := ft.Tag.Get("db")
			//auto increment field
			if ft.Tag.Get("pk") == "true" || isPkOrAi(dbTag, "pk") {
				if ft.Tag.Get("ai") == "true" || isPkOrAi(dbTag, "ai") {
					pks[n] = v.Field(k)
					ais[n] = true
					continue
				}
			}

			//auto update filed, created_at, updated_at, etc.
			if ft.Tag.Get("ignore") == "true" || ft.Tag.Get("or") != "" {
				continue
			}

			if !isFirst {
				vals.WriteString(",")
			}
			vals.WriteString("?")
			isFirst = false
			r := v.Field(k).Addr().Interface()
			if v.Field(k).Type().String() == "time.Time" {
				if r.(*time.Time).IsZero() {
					r = &zeroTime
				}
			}
			ret = append(ret, r)
		}
		vals.WriteString(")")
	}

	return cols, vals.String(), ret, pks, ais
}

func insert(c context.Context, tdx Tdx, s interface{}) error {
	return insertByTable(c, tdx, getTableName(s), s)
}

func insertByTable(c context.Context, tdx Tdx, tableName string, s interface{}) error {
	cols, vals, ifs, pk, isAi, _ := columnsByStruct(s)
	ret, err := exec(c, tdx, fmt.Sprintf("insert into %s (%s) values(%s)", tableName, cols, vals), ifs...)
	if err != nil {
		return err
	}
	if isAi {
		lid, err := ret.LastInsertId()
		if err != nil {
			return err
		}
		pk.SetInt(lid)
	}
	return nil
}

//更新或者插入，on duplicate key,	其中keys只支持写入数据库对应的字段
func insertOrUpdate(c context.Context, tdx Tdx, s interface{}, fields []string) error {
	cols, vals, ifs, pk, isAi, pkName := columnsByStruct(s)
	//重复时，需要更新的字段
	for k, v := range fields {
		v = fieldName2ColName(v)
		str := fmt.Sprintf("%s=values(%s)", v, v)
		fields[k] = str
	}
	//检查主键的情况，在insert中加入主键
	if pk.Addr().Interface() != nil {
		cols += fmt.Sprintf(",%s", pkName)
		vals += ",?"
		ifs = append(ifs, pk.Addr().Interface())
	}
	q := fmt.Sprintf("insert into %s (%s) values (%s) on duplicate key update %s", getTableName(s), cols, vals, strings.Join(fields, ","))
	ret, err := exec(c, tdx, q, ifs...)
	if err != nil {
		return err
	}
	if isAi {
		lid, err := ret.LastInsertId()
		if err != nil {
			return err
		}
		pk.SetInt(lid)
	}
	return nil
}

//通过传递需要更新的字段,去更新部分字段
func updateFieldsByPK(c context.Context, tdx Tdx, s interface{}, cols []string) error {
	ifs, pk, _, pkName := columnsByStructFields(s, cols)
	cs := make([]string, 0)
	for _, col := range cols {
		cs = append(cs, fieldName2ColName(col)+" = ?")
	}
	sv := strings.Join(cs, ",")
	q := fmt.Sprintf("update %s set %s where %s = %d", getTableName(s), sv, pkName, pk.Int())
	_, err := exec(c, tdx, q, ifs...)
	if err != nil {
		return err
	}
	return nil
}

func updateByPK(c context.Context, tdx Tdx, s interface{}) error {
	colStr, _, ifs, pk, _, pkName := columnsByStruct(s)
	cols := strings.Split(colStr, ",")
	cs := make([]string, 0)
	for _, col := range cols {
		cs = append(cs, col+" = ?")
	}
	sv := strings.Join(cs, ",")
	q := fmt.Sprintf("update %s set %s where %s = %d", getTableName(s), sv, pkName, pk.Int())
	_, err := exec(c, tdx, q, ifs...)
	if err != nil {
		return err
	}
	return nil
}

func insertBatch(c context.Context, tdx Tdx, s []interface{}) error {
	if s == nil || len(s) == 0 {
		return nil
	}
	//todo 需要check s中的数据都是同一种类型
	cols, vals, ifs, pks, ais := columnsBySlice(s)

	q := fmt.Sprintf("insert into %s %s values %s", getTableName(s[0]), cols, vals)
	ret, err := exec(c, tdx, q, ifs...)
	if err != nil {
		return err
	}
	//获取批量插入的last insert id, 然后给每个s[i]主键赋值
	lastInsertId, err := ret.LastInsertId()
	if err != nil {
		return err
	}
	for i, _ := range s {
		if ais[i] {
			pks[i].SetInt(lastInsertId + int64(i))
		}
	}
	return nil
}

type ORMer interface {
	WithContext(c context.Context) ORMer
	SelectOne(interface{}, string, ...interface{}) error
	SelectByPK(interface{}, interface{}) error
	Select(interface{}, string, ...interface{}) error
	SelectStr(string, ...interface{}) (string, error)
	SelectInt(string, ...interface{}) (int64, error)
	UpdateByPK(interface{}) error
	UpdateFieldsByPK(interface{}, []string) error
	Insert(interface{}) error
	InsertOrUpdate(interface{}, []string) error
	InsertBatch([]interface{}) error
	Exec(string, ...interface{}) (sql.Result, error)
	Query(string, ...interface{}) (*sql.Rows, error)
	ExecWithParam(string, interface{}) (sql.Result, error)
	ExecWithRowAffectCheck(int64, string, ...interface{}) error
}

type ORM struct {
	ctx context.Context

	db *sql.DB

	tables map[string]interface{}
}

func (o *ORM) WithContext(c context.Context) *ORM {
	no := new(ORM)
	*no = *o
	no.ctx = c
	return no
}

func NewORM(ds string) *ORM {
	return newORMWithDriver(ds, "mysql")
}

func NewPrestoORM(ds string) *ORM {
	return newORMWithDriver(ds, "prestgo")
}

func newORMWithDriver(ds string, driverName string) *ORM {
	initOnce.Do(func() {
		sqlParamReg, _ = regexp.Compile("(#{[a-zA-Z0-9-_]*})")
	})
	ret := &ORM{
		db:     nil,
		tables: make(map[string]interface{}),
	}
	var err error
	ret.db, err = sql.Open(driverName, ds)
	if err != nil {
		logrus.WithError(err).Fatal("Can not connect to db")
	}
	ret.db.SetMaxOpenConns(100)
	ret.db.SetMaxIdleConns(5)
	ret.db.SetConnMaxLifetime(time.Minute * 10)
	return ret
}

func (o *ORM) Close() error {
	return o.db.Close()
}

func (o *ORM) AddTable(s interface{}) {
	name := getTableName(s)
	o.tables[name] = s
}

func (o *ORM) CheckTables() {
	for _, s := range o.tables {
		err := checkTableColumns(o.db, s)
		if err != nil {
			logrus.WithError(err).Fatal("Can not pass table check")
		}
	}
}

func (o *ORM) GetTableByName(name string) interface{} {
	ret, ok := o.tables[name]
	if !ok {
		return nil
	} else {
		return ret
	}
}

func (o *ORM) TruncateTable(t string) error {
	_, err := o.db.Exec("truncate table " + t)
	return err
}

func (o *ORM) TruncateTables() error {
	for t, _ := range o.tables {
		err := o.TruncateTable(t)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *ORM) Begin() (*ORMTran, error) {
	tx, err := o.db.Begin()
	return &ORMTran{
		ctx: o.ctx,
		tx:  tx,
	}, err
}

func (o *ORM) SelectOne(s interface{}, query string, args ...interface{}) error {
	return selectOne(o.ctx, o.db, s, query, args...)
}

func (o *ORM) SelectByPK(s interface{}, pk interface{}) error {
	return selectByPK(o.ctx, o.db, s, pk)
}

func (o *ORM) Select(s interface{}, query string, args ...interface{}) error {
	return selectMany(o.ctx, o.db, s, query, args...)
}

func (o *ORM) SelectRawSet(query string, columnMaps map[string]string, args ...interface{}) ([]map[string]interface{}, error) {
	return selectRawSet(o.ctx, o.db, query, columnMaps, args...)
}

func (o *ORM) SelectRaw(query string, args ...interface{}) ([]string, [][]interface{}, error) {
	return selectRaw(o.ctx, o.db, query, args...)
}

func (o *ORM) SelectRawWithParam(paramQuery string, paramMap interface{}) ([]string, [][]interface{}, error) {
	return selectRawWithParam(o.ctx, o.db, paramQuery, paramMap)
}

func (o *ORM) SelectRawSetWithParam(paramQuery string, paramMap interface{}) ([]map[string]interface{}, error) {
	return selectRawSetWithParam(o.ctx, o.db, paramQuery, paramMap)
}

func (o *ORM) SelectStr(query string, args ...interface{}) (string, error) {
	return selectStr(o.ctx, o.db, query, args...)
}

func (o *ORM) SelectInt(query string, args ...interface{}) (int64, error) {
	return selectInt(o.ctx, o.db, query, args...)
}

func (o *ORM) UpdateByPK(s interface{}) error {
	return updateByPK(o.ctx, o.db, s)
}

//在数据库字段和struct字段不是以驼峰表示法对应的时候就会报错，建议填入数据库对应的字段
func (o *ORM) UpdateFieldsByPK(s interface{}, fields []string) error {
	return updateFieldsByPK(o.ctx, o.db, s, fields)
}

func (o *ORM) Insert(s interface{}) error {
	return insert(o.ctx, o.db, s)
}

func (o *ORM) InsertWithTable(s interface{}, tableName string) error {
	return insertByTable(o.ctx, o.db, tableName, s)
}

func (o *ORM) InsertBatch(s []interface{}) error {
	return insertBatch(o.ctx, o.db, s)
}

func (o *ORM) InsertOrUpdate(s interface{}, keys []string) error {
	return insertOrUpdate(o.ctx, o.db, s, keys)
}

func (o *ORM) ExecWithRowAffectCheck(n int64, query string, args ...interface{}) error {
	return execWithRowAffectCheck(o.ctx, o.db, n, query, args...)
}

func (o *ORM) Exec(query string, args ...interface{}) (sql.Result, error) {
	return exec(o.ctx, o.db, query, args...)
}
func (o *ORM) Query(queryStr string, args ...interface{}) (*sql.Rows, error) {
	return query(o.ctx, o.db, queryStr, args...)
}

func (o *ORM) ExecWithParam(paramQuery string, paramMap interface{}) (sql.Result, error) {
	return execWithParam(o.ctx, o.db, paramQuery, paramMap)
}

func getFieldValue(param interface{}, fieldName string) (interface{}, error) {
	v := reflect.ValueOf(param)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Map {
		fv := reflect.ValueOf(fieldName)
		f := v.MapIndex(fv)
		if f.IsValid() {
			return f.Interface(), nil
		} else {
			return nil, errors.New("missing field " + fieldName)
		}
	} else if v.Kind() == reflect.Struct {
		f := v.FieldByName(fieldName)
		var x []int
		x = append(x, 6)
		if f.IsValid() {
			return f.Interface(), nil
		} else {
			return nil, errors.New("missing field " + fieldName)
		}
	} else {
		return nil, errors.New(fmt.Sprintf("input interface type {%v} is not supported", v.Kind().String()))
	}
}

func (o *ORM) DoTransaction(f func(*ORMTran) error) error {
	trans, err := o.Begin()
	if err != nil {
		return err
	}
	defer func() {
		perr := recover()
		if err != nil || perr != nil {
			trans.Rollback()
			if perr != nil {
				panic(perr)
			}
			return
		} else {
			err = trans.Commit()
			return
		}
	}()
	err = f(trans)
	return err
}

func (o *ORM) DoTransactionMore(f func(*ORMTran) (interface{}, error)) (interface{}, error) {
	trans, err := o.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			trans.Rollback()
			return
		} else {
			err = trans.Commit()
			return
		}
	}()
	return f(trans)
}

type ORMTran struct {
	ctx context.Context
	tx  *sql.Tx
}

func (o *ORMTran) SelectOne(s interface{}, query string, args ...interface{}) error {
	return selectOne(o.ctx, o.tx, s, query, args...)
}

func (o *ORMTran) Insert(s interface{}) error {
	return insert(o.ctx, o.tx, s)
}

func (o *ORMTran) InsertOrUpdate(s interface{}, keys []string) error {
	return insertOrUpdate(o.ctx, o.tx, s, keys)
}

func (o *ORMTran) InsertBatch(s []interface{}) error {
	return insertBatch(o.ctx, o.tx, s)
}

func (o *ORMTran) UpdateByPK(s interface{}) error {
	return updateByPK(o.ctx, o.tx, s)
}

//在数据库字段和struct字段不是以驼峰表示法对应的时候就会报错，建议填入数据库对应的字段
func (o *ORMTran) UpdateFieldsByPK(s interface{}, fields []string) error {
	return updateFieldsByPK(o.ctx, o.tx, s, fields)
}
func (o *ORMTran) Exec(query string, args ...interface{}) (sql.Result, error) {
	return exec(o.ctx, o.tx, query, args...)
}

func (o *ORMTran) Query(queryStr string, args ...interface{}) (*sql.Rows, error) {
	return query(o.ctx, o.tx, queryStr, args...)
}

func (o *ORMTran) Commit() error {
	return o.tx.Commit()
}

func (o *ORMTran) Rollback() error {
	return o.tx.Rollback()
}

func (o *ORMTran) SelectByPK(s interface{}, pk interface{}) error {
	return selectByPK(o.ctx, o.tx, s, pk)
}

func (o *ORMTran) Select(s interface{}, query string, args ...interface{}) error {
	return selectMany(o.ctx, o.tx, s, query, args...)
}

func (o *ORMTran) SelectInt(query string, args ...interface{}) (int64, error) {
	return selectInt(o.ctx, o.tx, query, args...)
}

func (o *ORMTran) SelectStr(query string, args ...interface{}) (string, error) {
	return selectStr(o.ctx, o.tx, query, args...)
}

func (o *ORMTran) ExecWithParam(paramQuery string, paramMap interface{}) (sql.Result, error) {
	return execWithParam(o.ctx, o.tx, paramQuery, paramMap)
}

func (o *ORMTran) ExecWithRowAffectCheck(n int64, query string, args ...interface{}) error {
	return execWithRowAffectCheck(o.ctx, o.tx, n, query, args...)
}

func NormalizeValue(valueType string, value interface{}) (interface{}, error) {
	logrus.WithField("type", reflect.TypeOf(value)).WithField("value", reflect.ValueOf(value)).Info("NormalizeValue")
	switch value.(type) {
	case string:
		return value.(string), nil
	case []byte:
		str := string(value.([]byte))
		switch valueType {
		case "int64":
			return strconv.ParseInt(str, 10, 64)
		case "uint64":
			return strconv.ParseUint(str, 10, 64)
		case "float64":
			return strconv.ParseFloat(str, 64)
		case "int":
			v, e := strconv.ParseInt(str, 10, 64)
			if e != nil {
				return 0, e
			} else {
				return int(v), nil
			}
		case "uint":
			v, e := strconv.ParseUint(str, 10, 64)
			if e != nil {
				return 0, e
			} else {
				return uint(v), nil
			}
		default:
			return str, nil
		}
	case time.Time:
		return value.(time.Time), nil
	case nil:
		return nil, nil
	}
	return value, nil
}

func IsRowAffectError(err error) bool {
	return strings.HasPrefix(err.Error(), "[RowAffectCheckError]")
}
