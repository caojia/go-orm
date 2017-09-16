package main

import (
	"fmt"
	"reflect"
)

func main()  {
	i:="adf"
	a(i)
}
func a(a interface{})  {
	fmt.Println("tyepof",reflect.TypeOf(a))
	value:=reflect.ValueOf(a)
	fmt.Println("value of type",value.Type())
}
