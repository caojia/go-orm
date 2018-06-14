package orm

import (
	"errors"
	"reflect"
	"regexp"
	"strings"

	"github.com/sirupsen/logrus"
)

//替换query 中？？为长度为len的？
func getNumInStr(len int, query string) string {
	str := ""
	for i := 0; i < len; i++ {
		str += "?"
		if i != len-1 {
			str += ","
		}
	}
	return strings.Replace(query, "??", str, 1)
}

//检测sql中是否存在in，是否在args中存在数组和??这个关键词，如果都存在就进行拉平
func changeSQLIn(sql string, args ...interface{}) (string, []interface{}) {
	newArgs := make([]interface{}, 0)
	if strings.Contains(sql, "??") {
		//只有in的时候，对args中的数组进行处理
		for _, arg := range args {
			switch reflect.TypeOf(arg).Kind() {
			case reflect.Slice:
				s := reflect.ValueOf(arg)
				sql = getNumInStr(s.Len(), sql)
				for i := 0; i < s.Len(); i++ {
					newArgs = append(newArgs, s.Index(i).Interface())
				}
			default:
				newArgs = append(newArgs, arg)
			}
		}
	} else {
		newArgs = append(newArgs, args...)
	}
	return sql, newArgs
}

//检测select 的sql中的select函数时候存在limit，0表示就补上limit 2000，1表示补上limit 1
func addLimit(sql string, limitStatus int) string {
	//判断select是否有limit这个关键字,检查子查询
	if ok, _ := regexp.MatchString(`(?i)limit|^(?i)show`, sql); ok {
		return sql
	}
	sql = strings.TrimSuffix(sql, ";")
	//最后一个匹配项
	switch limitStatus {
	case 0:
		sql += " LIMIT 2000 "
		logrus.WithField("sql", sql).WithField("add limit", "LIMIT 2000").Warn("This sql does not have a limit condition, please add a limit. Automatically add limit for sql")
	case 1:
		sql += " LIMIT 1 "
	}
	return sql
}

//判断主键和自增列是否存在，存在就返回true，否则返回false
func isPkOrAi(dbTag string, str string) bool {
	if dbTag == "" {
		return false
	}
	arr := strings.Split(dbTag, ",")
	if len(arr) == 1 {
		if dbTag == str {
			return true
		}
		return false
	}
	ok := false
	for _, v := range arr {
		if v == str {
			ok = true
		}
	}
	return ok
}

//获取db标签中的col
func getDbTagCol(dbTag string) string {
	if dbTag == "" {
		return ""
	}
	arr := strings.Split(dbTag, ",")
	if len(arr) == 1 {
		if dbTag != "ai" && dbTag != "pk" {
			return dbTag
		}
		return ""
	}
	for _, v := range arr {
		if v != "ai" && v != "pk" {
			return v
		}
	}
	return ""
}

//把struct解析为map，规则是 pk -> cols ,ai->bool ,cols -> db tag ,field -> cols
func reflectStructToMap(s interface{}) (map[string]interface{}, error) {
	t := reflect.TypeOf(s)
	if t.Kind() == reflect.Map { //支持对map进行处理
		if temp, ok := s.(map[string]interface{}); ok {
			return temp, nil
		} else {
			return nil, errors.New("unsupported this map type:" + t.String() + ", supported map[string]interface{} only")
		}
	}
	v := reflect.ValueOf(s)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	return reflect2Map(v, t)
}

func reflect2Map(v reflect.Value, t reflect.Type) (map[string]interface{}, error) {
	//对struct中进行映射处理
	if v.Kind() != reflect.Struct { //首先对参数进行
		return nil, errors.New("struct is non-struct")
	}
	if t.NumField() < 1 {
		return nil, errors.New("store struct is empty")
	}
	ret := make(map[string]interface{}, t.NumField()+2)
	//对field进行处理
	for k := 0; k < t.NumField(); k++ {
		ft := t.Field(k)
		//auto update filed, created_at, updated_at, etc.
		if ft.Tag.Get("ignore") == "true" || ft.Tag.Get("or") != "" {
			continue
		}
		//先对主键进行处理
		if ft.Tag.Get("pk") == "true" {
			if ft.Tag.Get("db") != "" {
				ret["pk"] = ft.Tag.Get("db")
			} else {
				ret["pk"] = fieldName2ColName(ft.Name)
			}
			if ft.Tag.Get("ai") == "true" {
				ret["ai"] = 1
			} else {
				ret["ai"] = 0
			}
		}
		//有两种处理策略，当有db 标签是 map 里面的key就是db标签的值，否则就是struct中field利用驼峰进行转化
		col := ""
		if ft.Tag.Get("db") != "" {
			col = ft.Tag.Get("db")
		} else {
			col = fieldName2ColName(ft.Name)
		}
		//对struct中的值进行处理
		if v.Field(k).CanAddr() {
			ret[col] = v.Field(k).Addr().Interface()
		} else {
			ret[col] = v.Field(k).Interface()
		}
	}
	return ret, nil
}

//批量映射strut到map
func reflectListStructToMap(sarray interface{}) ([]map[string]interface{}, error) {
	ret := make([]map[string]interface{}, 0)

	t := reflect.TypeOf(sarray)
	v := reflect.ValueOf(sarray)

	if v.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	kind := v.Kind()

	if kind != reflect.Array && kind != reflect.Slice {
		return ret, errors.New(" interface{} is non-array or non-slice")
	}

	length := v.Len()

	if length < 1 {
		return ret, errors.New("interface{} is empty")
	}
	for i := 0; i < length; i++ {
		val := v.Index(i)

		rv, re := reflect2Map(val, val.Type())
		if re != nil {
			return ret, errors.New("error:" + re.Error())
		}
		ret = append(ret, rv)
	}

	return ret, nil
}
