package main

import (
	"key-value-engine/structs/btree"
	"key-value-engine/structs/record"
	"key-value-engine/structs/sstable"
)

func main() {
	bt, _ := btree.MakeBTree(4)
	keys := []string{"20", "10", "18", "12", "28", "2", "16", "15", "22", "100", "2000", "8", "6", "4", "13", "1", "17", "25"}
	//keys := []string{"73", "41", "89", "56", "32", "67", "95", "78", "13", "62", "53", "29", "61", "44"}
	//keys := []string{"83", "29", "46", "92", "15", "37", "69", "74", "60", "5", "66", "68", "97", "88", "40", "24"}

	for _, key := range keys {
		tomb := false
		if key == "97" || key == "15" {
			tomb = true
		}
		rec := record.MakeRecord(key, []byte(key), tomb)

		bt.Insert(rec)
	}

	sst, _ := sstable.MakeSSTable(5, false, 0.1, true)

	//err := sst.Flush(bt.GetSorted())
	//if err != nil {
	//	return
	//}

	for _, key := range keys {
		get, _ := sst.Get(key)

		if get != nil {
			get.PrintRecord()

		} else {
			//fmt.Println(err)
			print("EERRR######################################################################################")
		}
	}
}
