package main

import (
	"key-value-engine/structs/Engine"
)

func main() {
	e := Engine.MakeEngine()
	e.Main()

	//e.PopulateScript(1000, 100)
}
