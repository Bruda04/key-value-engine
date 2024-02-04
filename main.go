package main

import (
	"key-value-engine/structs/Engine"
)

func main() {
	e := Engine.MakeEngine()
	e.Main()
	//s := time.Now()
	//e.PopulateScript(1000, 100)
	//fmt.Println(time.Now().Sub(s))
}
