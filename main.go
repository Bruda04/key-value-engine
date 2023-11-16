package main

import (
	"fmt"
	"key-value-engine/structs/cms"
)

func main() {
	cms := cms.MakeCMS(0.1, 0.1)
	cms.Add([]byte("Luka"))
	cms.Add([]byte("Marija"))
	cms.Add([]byte("Ivana"))
	cms.Add([]byte("Luka"))
	cms.Add([]byte("Luka"))
	cms.Add([]byte("Luka"))
	fmt.Println(cms.Estimate([]byte("Luka")))
	fmt.Println(cms.Estimate([]byte("Ivana")))
	fmt.Println(cms.Estimate([]byte("Marija")))

}
