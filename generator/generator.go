package generator

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/mijia/modelq/drivers"
)

type CodeResult struct {
	name string
	err  error
}

type CodeConfig struct {
	PackageName    string
	TouchTimestamp bool
	Template       string
	SkipPrefix     string
}

func (cc CodeConfig) MustCompileTemplate() *template.Template {
	if cc.Template == "" {
		return nil
	}
	return template.Must(template.ParseFiles(cc.Template))
}

func GenerateModels(dbName string, dbSchema drivers.DbSchema, config CodeConfig) {
	customTmpl := config.MustCompileTemplate()

	if fs, err := os.Stat(config.PackageName); err != nil || !fs.IsDir() {
		os.Mkdir(config.PackageName, os.ModeDir|os.ModePerm)
	}

	jobs := make(chan CodeResult)
	for tbl, cols := range dbSchema {
		go func(tableName string, schema drivers.TableSchema) {
			shortTableName := tableName
			if strings.HasPrefix(tableName, config.SkipPrefix) {
				shortTableName = tableName[len(config.SkipPrefix):]
			}
			err := generateModel(dbName, shortTableName, tableName, schema, config, customTmpl)
			jobs <- CodeResult{tableName, err}
		}(tbl, cols)
	}

	for i := 0; i < len(dbSchema); i++ {
		result := <-jobs
		if result.err != nil {
			log.Printf("Error when generating code for %s, %s", result.name, result.err)
		} else {
			log.Printf("Code generated for table %s, into package %s/%s.go", result.name, config.PackageName, result.name)
		}
	}
	close(jobs)
}

func generateModel(dbName, shortTName, tName string, schema drivers.TableSchema, config CodeConfig, tmpl *template.Template) error {
	// omit table with "_" as prefix, such as _yoyo_migrations
	if strings.HasPrefix(tName, "_") {
		return nil
	}
	file, err := os.Create(path.Join(config.PackageName, shortTName+".go"))
	if err != nil {
		return err
	}
	w := bufio.NewWriter(file)

	defer func() {
		w.Flush()
		file.Close()
	}()

	model := ModelMeta{
		Name:      toCapitalCase(shortTName, true),
		LowerName: toCapitalCase(shortTName, false),
		DbName:    dbName,
		TableName: tName,
		Fields:    make([]ModelField, len(schema)),
		Uniques:   make([]ModelField, 0, len(schema)),
		config:    config,
	}
	needTime := false
	for i, col := range schema {
		field := ModelField{
			Name:            toCapitalCase(col.ColumnName, true),
			ColumnName:      col.ColumnName,
			Type:            col.DataType,
			Tag:             "",
			IsPrimaryKey:    strings.ToUpper(col.ColumnKey) == "PRI",
			IsUniqueKey:     strings.ToUpper(col.ColumnKey) == "UNI",
			IsAutoIncrement: strings.ToUpper(col.Extra) == "AUTO_INCREMENT",
			DefaultValue:    col.DefaultValue,
			Extra:           col.Extra,
			Comment:         col.Comment,
		}
		if field.Type == "time.Time" {
			needTime = true
			field.Formatter = ".Format(time.RFC3339)"
			field.DefaultValueCode = "time.Now()"
		} else if strings.Contains(field.Type, "int") || strings.Contains(field.Type, "float") {
			field.DefaultValueCode = "0"
		} else if field.Type == "string" {
			field.DefaultValueCode = "\"\""
		}
		dbTag := ""
		tagArr := make([]string, 0)
		//增加json和db标签
		jsonTag := fmt.Sprintf("json:\"%s\"", col.ColumnName)
		tagArr = append(tagArr, jsonTag)
		if field.IsPrimaryKey {
			tagArr = append(tagArr, "pk:\"true\"")
			if model.PrimaryField != nil {
				return fmt.Errorf("must not have more than one primary keys, %+v", field)
			}
			model.PrimaryField = &field
			if field.IsAutoIncrement {
				dbTag = fmt.Sprintf("db:\"%s,ai,pk\"", col.ColumnName)
				tagArr = append(tagArr, "ai:\"true\"")
			} else {
				dbTag = fmt.Sprintf("db:\"%s,pk\"", col.ColumnName)
			}
		} else {
			dbTag = fmt.Sprintf("db:\"%s\"", col.ColumnName)
		}

		if col.ColumnName == "created_at" || col.ColumnName == "updated_at" {
			tagArr = append(tagArr, "ignore:\"true\"")
		}
		if len(tagArr) > 0 {
			tagArr = append(tagArr, dbTag)
			field.Tag = fmt.Sprintf("`%s`", strings.Join(tagArr, " "))
		}
		if field.IsUniqueKey {
			model.Uniques = append(model.Uniques, field)
		}

		model.Fields[i] = field
	}

	if err := model.GenHeader(w, tmpl, needTime); err != nil {
		return fmt.Errorf("[%s] Fail to gen model header, %s", tName, err)
	}
	if err := model.GenStruct(w, tmpl); err != nil {
		return fmt.Errorf("[%s] Fail to gen model struct, %s", tName, err)
	}
	if err := model.GenObjectApi(w, tmpl); err != nil {
		return fmt.Errorf("[%s] Fail to gen model object api, %s", tName, err)
	}

	testFileName := path.Join(config.PackageName, shortTName+"_test.go")
	if err := generateModelTest(model, tmpl, needTime, testFileName); err != nil {
		return err
	}

	return nil
}

func generateModelTest(model ModelMeta, tmpl *template.Template, needTime bool, fileName string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(file)

	defer func() {
		w.Flush()
		file.Close()
	}()

	if err := model.GenTestHeader(w, tmpl, needTime); err != nil {
		return fmt.Errorf("[%s] Fail to gen model test code header, %s", model.TableName, err)
	}

	if err := model.GenTestCode(w, tmpl); err != nil {
		return fmt.Errorf("[%s] Fail to gen model test code, %s", model.TableName, err)
	}
	return nil
}

type ModelField struct {
	Name             string
	ColumnName       string
	Type             string
	Tag              string
	IsPrimaryKey     bool
	IsUniqueKey      bool
	IsAutoIncrement  bool
	DefaultValue     string
	Extra            string
	Comment          string
	Formatter        string
	DefaultValueCode string
}

func (f ModelField) ConverterFuncName() string {
	convertors := map[string]string{
		"int64":     "AsInt64",
		"int":       "AsInt",
		"string":    "AsString",
		"time.Time": "AsTime",
		"float64":   "AsFloat64",
		"bool":      "AsBool",
	}
	if c, ok := convertors[f.Type]; ok {
		return c
	}
	return "AsString"
}

type PrimaryFields []*ModelField

func (pf PrimaryFields) FormatObject() func(string) string {
	return func(name string) string {
		// "<Article ArticleId=%v UserId=%v>", obj.ArticleId, obj.UserId
		formats := make([]string, len(pf))
		for i, field := range pf {
			formats[i] = fmt.Sprintf("%s=%%v", field.Name)
		}
		outputs := make([]string, 1+len(pf))
		outputs[0] = fmt.Sprintf("\"<%s %s>\"", name, strings.Join(formats, " "))
		for i, field := range pf {
			outputs[i+1] = fmt.Sprintf("obj.%s", field.Name)
		}
		return strings.Join(outputs, ", ")
	}
}

func (pf PrimaryFields) FormatIncrementId() func() string {
	// obj.Id = {{if eq .PrimaryField.Type "int64"}}id{{else}}{{.PrimaryField.Type}}(id){{end}}
	return func() string {
		for _, field := range pf {
			if field.IsAutoIncrement {
				if field.Type == "int64" {
					return fmt.Sprintf("obj.%s = id", field.Name)
				} else {
					return fmt.Sprintf("obj.%s = %s(id)", field.Name, field.Type)
				}
			}
		}
		return ""
	}
}

func (pf PrimaryFields) FormatFilters() func(string) string {
	// filter := {{.Name}}Objs.Filter{{.PrimaryField.Name}}("=", obj.{{.PrimaryField.Name}})
	return func(name string) string {
		filters := make([]string, len(pf))
		for i, field := range pf {
			if i == 0 {
				filters[i] = fmt.Sprintf("filter := %sObjs.Filter%s(\"=\", obj.%s)", name, field.Name, field.Name)
			} else {
				filters[i] = fmt.Sprintf("filter = filter.And(%sObjs.Filter%s(\"=\", obj.%s))", name, field.Name, field.Name)
			}
		}
		return strings.Join(filters, "\n")
	}
}

type ModelMeta struct {
	Name         string
	LowerName    string
	DbName       string
	TableName    string
	PrimaryField *ModelField
	Fields       []ModelField
	Uniques      []ModelField
	config       CodeConfig
}

func (m ModelMeta) AllFields() string {
	fields := make([]string, len(m.Fields))
	for i, f := range m.Fields {
		fields[i] = fmt.Sprintf("\"%s\"", f.Name)
	}
	return strings.Join(fields, ", ")
}

func (m ModelMeta) InsertableFields() string {
	fields := make([]string, 0, len(m.Fields))
	for _, f := range m.Fields {
		if f.IsPrimaryKey && f.IsAutoIncrement {
			continue
		}
		autoTimestamp := strings.ToUpper(f.DefaultValue) == "CURRENT_TIMESTAMP" ||
			strings.ToUpper(f.DefaultValue) == "NOW()"
		if f.Type == "time.Time" && autoTimestamp && !m.config.TouchTimestamp {
			continue
		}
		fields = append(fields, fmt.Sprintf("\"%s\"", f.Name))
	}
	return strings.Join(fields, ", ")
}

func (m ModelMeta) GetInsertableFields() []ModelField {
	fields := make([]ModelField, 0, len(m.Fields))
	for _, f := range m.Fields {
		if f.IsPrimaryKey && f.IsAutoIncrement {
			continue
		}
		autoTimestamp := strings.ToUpper(f.DefaultValue) == "CURRENT_TIMESTAMP" ||
			strings.ToUpper(f.DefaultValue) == "NOW()"
		if f.Type == "time.Time" && autoTimestamp && !m.config.TouchTimestamp {
			continue
		}
		fields = append(fields, f)
	}
	return fields
}

func (m ModelMeta) UpdatableFields() string {
	fields := make([]string, 0, len(m.Fields))
	for _, f := range m.Fields {
		if f.IsPrimaryKey {
			continue
		}
		autoUpdateTime := strings.ToUpper(f.Extra) == "ON UPDATE CURRENT_TIMESTAMP"
		if autoUpdateTime && !m.config.TouchTimestamp {
			continue
		}
		fields = append(fields, fmt.Sprintf("\"%s\"", f.Name))
	}
	return strings.Join(fields, ", ")
}

func (m ModelMeta) GetUpdatableFields() []ModelField {
	fields := make([]ModelField, 0, len(m.Fields))
	for _, f := range m.Fields {
		if f.IsPrimaryKey {
			continue
		}
		autoUpdateTime := strings.ToUpper(f.Extra) == "ON UPDATE CURRENT_TIMESTAMP"
		if autoUpdateTime && !m.config.TouchTimestamp {
			continue
		}
		fields = append(fields, f)
	}
	return fields
}

func (m ModelMeta) getTemplate(tmpl *template.Template, name string, defaultTmpl *template.Template) *template.Template {
	if tmpl != nil {
		if definedTmpl := tmpl.Lookup(name); definedTmpl != nil {
			return definedTmpl
		}
	}
	return defaultTmpl
}

func (m ModelMeta) GenHeader(w *bufio.Writer, tmpl *template.Template, importTime bool) error {
	return m.getTemplate(tmpl, "header", tmHeader).Execute(w, map[string]interface{}{
		"DbName":     m.DbName,
		"TableName":  m.TableName,
		"PkgName":    m.config.PackageName,
		"ImportTime": importTime,
	})
}

func (m ModelMeta) GenTestHeader(w *bufio.Writer, tmpl *template.Template, importTime bool) error {
	return m.getTemplate(tmpl, "test_header", tmTestHeader).Execute(w, map[string]interface{}{
		"DbName":     m.DbName,
		"TableName":  m.TableName,
		"PkgName":    m.config.PackageName,
		"ImportTime": importTime,
	})
}

func (m ModelMeta) GenStruct(w *bufio.Writer, tmpl *template.Template) error {
	return m.getTemplate(tmpl, "struct", tmStruct).Execute(w, m)
}

func (m ModelMeta) GenObjectApi(w *bufio.Writer, tmpl *template.Template) error {
	return m.getTemplate(tmpl, "obj_api", tmObjApi).Execute(w, m)
}

func (m ModelMeta) GenTestCode(w *bufio.Writer, tmpl *template.Template) error {
	return m.getTemplate(tmpl, "test_code", tmTestCode).Execute(w, m)
}

func toCapitalCase(name string, firstLetterUpper bool) string {
	// cp___hello_12jiu -> CpHello_12Jiu
	data := []byte(name)
	segStart := true
	endPos := 0
	isFirst := true
	lastUnderScore := false
	for i := 0; i < len(data); i++ {
		ch := data[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
			if segStart {
				if ch >= 'a' && ch <= 'z' {
					if !isFirst || firstLetterUpper {
						ch = ch - 'a' + 'A'
					}
				}
				segStart = false
			} else {
				if ch >= 'A' && ch <= 'Z' {
					ch = ch - 'A' + 'a'
				}
			}
			data[endPos] = ch
			lastUnderScore = false
			endPos++
		} else if ch >= '0' && ch <= '9' {
			if lastUnderScore {
				data[endPos] = "_"[0]
				endPos++
			}
			data[endPos] = ch
			endPos++
			segStart = true
			lastUnderScore = false
		} else {
			lastUnderScore = true
			segStart = true
		}
		isFirst = false
	}
	return string(data[:endPos])
}
