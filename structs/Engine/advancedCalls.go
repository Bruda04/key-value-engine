package Engine

import (
	"encoding/binary"
	"fmt"
	"key-value-engine/structs/bloomFilter"
	"key-value-engine/structs/cms"
	"key-value-engine/structs/hll"
	"key-value-engine/structs/simHash"
	"regexp"
	"strconv"
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
		displayError(err)
		return
	}

}

func (e *Engine) simhHash(call string) {
	parts := strings.Split(call, " ")
	name1 := parts[1]
	name2 := parts[2]
	key1 := "fingerprint " + name1
	key2 := "fingerprint " + name2

	rec1, err := e.readPath(key1)
	if err != nil {
		displayError(err)
		return
	}
	if rec1 != nil {
		if rec1.IsTombstone() {
			return
		}
	} else {
		return
	}
	rec2, err := e.readPath(key2)
	if err != nil {
		displayError(err)
		return
	}
	if rec2 != nil {
		if rec2.IsTombstone() {
			return
		}
	} else {
		return
	}

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
		regEl := regexp.MustCompile(`^\d+$`)
		regPrec := regexp.MustCompile(`^0\.\d+$`)
		fmt.Print("Enter expected elements: ")
		input := getInput()
		if !regEl.MatchString(input) {
			fmt.Println("invalid input")
			return
		}
		expectedEl, _ := strconv.ParseInt(input, 10, 64)
		fmt.Print("Enter precision: ")
		input = getInput()
		if !regPrec.MatchString(input) {
			fmt.Println("invalid input")
			return
		}
		precision, _ := strconv.ParseFloat(input, 64)

		obj := bloomFilter.MakeBloomFilter(uint64(expectedEl), precision)
		objBytes = obj.BloomFilterToBytes()

	} else if structure == "hll" {
		regPr := regexp.MustCompile(`^\d+$`)
		fmt.Print("Enter precision: ")
		input := getInput()
		if !regPr.MatchString(input) {
			fmt.Println("invalid input")
			return
		}
		precision, _ := strconv.ParseInt(input, 10, 64)
		obj, err := hll.MakeHLL(uint8(precision))
		if err != nil {
			fmt.Println("invalid input range")
			return
		}
		objBytes, _ = obj.HLLToBytes()
	} else if structure == "cms" {
		regFlt := regexp.MustCompile(`^0\.\d+$`)
		fmt.Print("Enter epsilon: ")
		input := getInput()
		if !regFlt.MatchString(input) {
			fmt.Println("invalid input")
			return
		}
		epsilon, _ := strconv.ParseFloat(input, 64)
		fmt.Print("Enter delta: ")
		input = getInput()
		if !regFlt.MatchString(input) {
			fmt.Println("invalid input")
			return
		}
		delta, _ := strconv.ParseFloat(input, 64)
		if epsilon <= 0 || delta <= 0 {
			fmt.Println("invalid input range")
			return
		}

		obj := cms.MakeCMS(epsilon, delta)
		objBytes = obj.CMSToBytes()
	}

	err := e.writePath(key, objBytes, false)
	if err != nil {
		displayError(err)
		return
	}
}

func (e *Engine) destroy(call string) {
	parts := strings.Split(call, " ")
	structure := parts[0]
	name := parts[2]

	key := structure + " " + name

	rec, err := e.readPath(key)
	if err != nil {
		displayError(err)
		return
	}
	if rec != nil {
		err = e.writePath(rec.GetKey(), rec.GetValue(), true)
		if err != nil {
			displayError(err)
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
		displayError(err)
		return
	}
	if objRec != nil {
		if objRec.IsTombstone() {
			return
		}
	} else {
		return
	}

	objBytes := objRec.GetValue()

	if structure == "bf" {
		obj, err := bloomFilter.BytesToBloomFilter(objBytes)
		if err != nil {
			displayError(err)
			return
		}
		obj.Add(value)
		objBytes = obj.BloomFilterToBytes()
	} else if structure == "hll" {
		obj, _ := hll.BytesToHLL(objBytes)
		err := obj.Add(value)
		if err != nil {
			displayError(err)
			return
		}
		objBytes, _ = obj.HLLToBytes()
	} else if structure == "cms" {
		obj, err := cms.BytesToCMS(objBytes)
		if err != nil {
			displayError(err)
			return
		}
		obj.Add(value)
		objBytes = obj.CMSToBytes()
	}

	err = e.writePath(key, objBytes, false)
	if err != nil {
		displayError(err)
		return
	}
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
		displayError(err)
		return
	}
	if objRec != nil {
		if objRec.IsTombstone() {
			return
		}
	} else {
		return
	}

	objBytes := objRec.GetValue()

	if structure == "bf" {
		obj, err := bloomFilter.BytesToBloomFilter(objBytes)
		if err != nil {
			displayError(err)
			return
		}
		fmt.Println(obj.IsPresent(value))
	} else if structure == "hll" {
		obj, err := hll.BytesToHLL(objBytes)
		if err != nil {
			displayError(err)
			return
		}
		fmt.Println(obj.Estimate())
	} else if structure == "cms" {
		obj, err := cms.BytesToCMS(objBytes)
		if err != nil {
			displayError(err)
			return
		}
		fmt.Println(obj.Estimate(value))
	}
}
