package main

import (
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os/exec"
	"strings"

	"github.com/caojia/go-orm/generator"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mijia/modelq/drivers"
)

var indexTm *template.Template

var formFields = map[string]string{
	"database": "数据库名",
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
		panic(err)
	}

}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		render(w, "")
		return
	}
	r.ParseForm()
	missingFields := make([]string, 0, len(formFields))
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
	password := r.Form.Get("password")
	if password != "" {
		password = ":" + password
	}
	dsn := fmt.Sprintf("%s%s@tcp(%s)/%s", user, password, host, r.Form.Get("database"))
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
    <div class='form_box'>
        <p style="color: red">{{ . }}</p>
        <form action="/" method="POST" class="form_table">
            <table>
                <tr>
                    <td>数据库类型:</td>
                    <td class="radio_box">
                        <input type="radio" name="driver" value="mysql" checked="true">
                        <span>mysql</span>
                        <input type="radio" name="driver" value="postgres">
                        <span>postgres</span>
                    </td>
                </tr>
                <tr>
                    <td>数据库host:</td>
                    <td>
                        <input type="text" name="host" placeholder="127.0.0.1:3306">
                    </td>
                </tr>
                <tr>
                    <td>数据库用户名:</td>
                    <td>
                        <input type="text" name="user" placeholder="root">
                    </td>
                </tr>
                <tr>
                    <td>
                        数据库密码:</td>
                    <td>
                        <input type="password" name="password">
                    </td>
                </tr>
                <tr>
                    <td>
                        数据库名<span class="red">*</span>:</td>
                    <td>
                        <input type="text" name="database">
                    </td>
                </tr>
                <tr>
                    <td>
                        表名<span class="red">*</span>:</td>
                    <td>
                        <input type="text" name="tables" placeholder='e.g. "user,article,blog"'>
                    </td>
                </tr>
                <tr>
                    <td>
                        生成代码的包名<span class="red">*</span>:</td>
                    <td>
                        <input type="text" name="packname" placeholder='e.g. "models"'>
                    </td>
                </tr>
                <tr>
                    <td>prefix:</td>
                    <td>
                        <input type="text" name="prefix" placeholder='Prefix to skip when generating the table models'>
                    </td>
                </tr>
            </table>
            <div class="form_submit">
                <input type="submit" value="提交">
            </div>
        </form>
        <form action="/close" onSubmit="setTimeout(function(){window.opener=null;window.close();})" class="form_exit">
            <input type="submit" value="退出" class="btn_gray">
        </form>
    </div>
</body>

<style>
    * {
        margin: 0;
        padding: 0;
    }

    .form_box {
        position: relative;
        width: 600px;
        margin: 0 auto;
        padding-bottom: 60px;
    }

    .form_box .red {
        vertical-align: middle;
        color: #ed3f14;
        margin-right: 5px;
    }

    .form_table table {
        width: 100%;
        border-collapse: separate;
        border-spacing: 0px 10px;
    }

    .form_table tr td:first-child {
        width: 140px;
        font-size: 16px;
        color: #666;
    }

    .form_table td {
        height: 40px;
    }

    .form_table input[type="text"],
    input[type="password"] {
        height: 100%;
        width: 100%;
        font-size: 14px;
        letter-spacing: 1px;
        border: 1px solid #999;
        border-radius: 8px;
        outline: none;
        padding: 0 8px;
        color: #666;
    }

    .form_table .radio_box {
        color: #666;
    }

    .form_table .radio_box span {
        margin-right: 70px;
    }

    .radio_box input[type="radio"] {
        vertical-align: middle;
    }

    input[type="submit"] {
        width: 100px;
        height: 40px;
        background: #2d8cf0;
        border: 1px solid transparent;
        cursor: pointer;
        color: #fff;
        line-height: 40px;
        border-radius: 6px;
        outline: none;
    }

    .form_submit {
        position: absolute;
        left: 140px;
        bottom: 0;
    }

    .form_exit{
        position: absolute;
        width: 100px;
        bottom: 0;
        right: 0;
    }

    .form_exit .btn_gray {
        background: rgb(187, 190, 196);
    }
</style>

</html>
`
