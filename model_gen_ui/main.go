package main

import (
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os/exec"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mijia/modelq/drivers"
	"github.com/mtxmn/go-orm/generator"
)

var indexTm *template.Template

var formFields = map[string]string{
	"database": "数据库名",
	"password": "数据库密码",
	"tables":   "表名",
	"packname": "生成代码的包名",
}

func init() {
	indexTm = template.Must(template.New("index").Parse(index))
}

func main() {
	var port string
	flag.StringVar(&port, "port", "3000", "port for model gen web ui, default is 3000")
	flag.Parse()
	http.HandleFunc("/close", closeHandler)
	http.HandleFunc("/", indexHandler)
	host := "localhost:" + port
	go openBrowser(host)
	err := http.ListenAndServe(host, nil)
	if err != nil {
		fmt.Println("you can provide a port by flag -port=3000")
		log.Panicf("start server failed, err: %s", err)
	}

}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		render(w, "")
		return
	}
	r.ParseForm()
	missingFields := make([]string, 0)
	for k, v := range formFields {
		if r.Form.Get(k) == "" {
			missingFields = append(missingFields, v)
		}
	}
	if len(missingFields) > 0 {
		render(w, strings.Join(missingFields, "、")+"必须填写！")
		return
	}
	user := r.Form.Get("user")
	if user == "" {
		user = "root"
	}
	host := r.Form.Get("host")
	if host == "" {
		host = "127.0.0.1:3306"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, r.Form.Get("password"), host, r.Form.Get("database"))
	dbSchema, err := drivers.LoadDatabaseSchema(r.Form.Get("driver"), dsn, r.Form.Get("database"), r.Form.Get("tables"))
	if err != nil {
		render(w, err.Error())
		return
	}
	codeConfig := &generator.CodeConfig{
		PackageName:    r.Form.Get("packname"),
		TouchTimestamp: false,
		Template:       "",
		SkipPrefix:     r.Form.Get("prefix"),
	}
	codeConfig.MustCompileTemplate()
	generator.GenerateModels(r.Form.Get("database"), dbSchema, *codeConfig)
	exec.Command("gofmt", "-w", r.Form.Get("packname")).Run()
	render(w, "生成代码成功")
}

func closeHandler(w http.ResponseWriter, r *http.Request) {
	log.Fatalf("close by front end page")
}

func openBrowser(host string) {
	log.Println("listening on", host)
	exec.Command("open", "http://"+host).Run()
}

func render(w http.ResponseWriter, prompt string) {
	indexTm.Execute(w, prompt)
}

var index = `
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="ie=edge">
    <title>Document</title>
</head>

<body>
    <div style="width: 80%; margin: auto auto">
        <p style="color: red">{{ . }}</p>
        <form action="/" method="POST">
            <table>
                <tr>
                    <td>数据库类型:</td>
                    <td>
                        mysql:
                        <input type="radio" name="driver" value="mysql" checked="true"></input>
                        postgres:
                        <input type="radio" name="driver" value="postgres"></input>
                    </td>
                </tr>
                <tr>
                    <td>数据库host:</td>
                    <td>
                        <input type="text" name="host" placeholder="127.0.0.1:3306"></input>
                    </td>
                </tr>
                <tr>
                    <td>数据库用户名:</td>
                    <td>
                        <input type="text" name="user" placeholder="root"></input>
                    </td>
                </tr>
                <tr>
                    <td>*数据库密码:</td>
                    <td>
                        <input type="password" name="password"></input>
                    </td>
                </tr>
                <tr>
                    <td>*数据库名:</td>
                    <td>
                        <input type="text" name="database"></input>
                    </td>
                </tr>
                <tr>
                    <td>*表名:</td>
                    <td>
                        <input type="text" name="tables" placeholder='e.g. "user,article,blog"'></input>
                    </td>
                </tr>
                <tr>
                    <td>*生成代码的包名:</td>
                    <td>
                        <input type="text" name="packname" placeholder='e.g. "models"'></input>
                    </td>
                </tr>
                <tr>
                    <td>prefix:</td>
                    <td>
                        <input type="text" name="prefix" placeholder='Prefix to skip when generating the table models'></input>
                    </td>
                </tr>
                <tr>
                    <td>
                        <input type="submit" value="提交" style="width:100px;height:25px"></input>
                    </td>
                </tr>
            </table>
		</form>
		<form action="/close" onSubmit="setTimeout(function(){window.opener=null;window.close();})">
			<input type="submit" value="退出" style="width:100px;height:25px"></input>
		</form>
    </div>
</body>

<style>
    input[type="text"], input[type="password"] {
        width: 500px
    }
</style>
</html>
`
