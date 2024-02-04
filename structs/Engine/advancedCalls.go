package Engine

import (
	"encoding/binary"
	"fmt"
	"key-value-engine/structs/bloomFilter"
	"key-value-engine/structs/cms"
	"key-value-engine/structs/hll"
	"key-value-engine/structs/simHash"
	"strings"
)

func (e *Engine) storeFingerprint(call string) {
	parts := strings.Split(call, " ")
	name := parts[1]
	text := strings.Join(parts[2:], " ")

	fingerprint := simHash.SimHash([]byte(text))
	fingerprintBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(fingerprintBytes, uint64(fingerprint))

	key := "fingerprint " + name

	err := e.writePath(key, fingerprintBytes, false)
	if err != nil {
		return
	}

}

func (e *Engine) simhHash(call string) {
	parts := strings.Split(call, " ")
	name1 := parts[1]
	name2 := parts[2]
	key1 := "fingerprint " + name1
	key2 := "fingerprint " + name2

	rec1, _ := e.readPath(key1)
	rec2, _ := e.readPath(key2)

	fingerprint1 := binary.LittleEndian.Uint64(rec1.GetValue())
	fingerprint2 := binary.LittleEndian.Uint64(rec2.GetValue())

	sh := simHash.HemingDistance(uint(fingerprint1), uint(fingerprint2))

	fmt.Println(sh)
}

func (e *Engine) makeStruct(call string) {
	parts := strings.Split(call, " ")
	structure := parts[0]
	name := parts[2]

	key := structure + " " + name

	var objBytes []byte
	if structure == "bf" {
		obj := bloomFilter.MakeBloomFilter(100, 0.1)
		objBytes = obj.BloomFilterToBytes()
	} else if structure == "hll" {
		obj, _ := hll.MakeHLL(12)
		objBytes, _ = obj.HLLToBytes()
	} else if structure == "cms" {
		obj := cms.MakeCMS(0.1, 0.9)
		objBytes = obj.CMSToBytes()
	}

	e.writePath(key, objBytes, false)
}

func (e *Engine) destroy(call string) {
	parts := strings.Split(call, " ")
	structure := parts[0]
	name := parts[2]

	key := structure + " " + name

	rec, err := e.readPath(key)
	if err != nil {
		return
	}
	if rec != nil {
		err = e.writePath(rec.GetKey(), rec.GetValue(), true)
		if err != nil {
			return
		}
	}
}

func (e *Engine) populateStruct(call string) {
	parts := strings.Split(call, " ")
	name := parts[2]
	structure := parts[0]
	value := []byte(strings.Join(parts[3:], " "))

	key := structure + " " + name

	objRec, err := e.readPath(key)
	if err != nil {
		return
	}

	objBytes := objRec.GetValue()

	if structure == "bf" {
		obj, _ := bloomFilter.BytesToBloomFilter(objBytes)
		obj.Add(value)
		objBytes = obj.BloomFilterToBytes()
	} else if structure == "hll" {
		obj, _ := hll.BytesToHLL(objBytes)
		obj.Add(value)
		objBytes, _ = obj.HLLToBytes()
	} else if structure == "cms" {
		obj, _ := cms.BytesToCMS(objBytes)
		obj.Add(value)
		objBytes = obj.CMSToBytes()
	}

	e.writePath(key, objBytes, false)
}

func (e *Engine) checkStruct(call string) {
	parts := strings.Split(call, " ")
	name := parts[2]
	structure := parts[0]
	var value []byte
	if structure != "hll" {
		value = []byte(strings.Join(parts[3:], " "))
	}

	key := structure + " " + name

	objRec, err := e.readPath(key)
	if err != nil {
		return
	}

	objBytes := objRec.GetValue()

	if structure == "bf" {
		obj, _ := bloomFilter.BytesToBloomFilter(objBytes)
		fmt.Println(obj.IsPresent(value))
	} else if structure == "hll" {
		obj, _ := hll.BytesToHLL(objBytes)
		fmt.Println(obj.Estimate())
	} else if structure == "cms" {
		obj, _ := cms.BytesToCMS(objBytes)
		fmt.Println(obj.Estimate(value))
	}
}
