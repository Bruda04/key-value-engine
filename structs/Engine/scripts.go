package Engine

import (
	"strconv"
)

func (e *Engine) PopulateScript(entries int, differentKeys int) {
	keys := make([]string, differentKeys)
	for j := 0; j < differentKeys; j++ {
		keys[j] = strconv.Itoa(j)
	}

	for i := 0; i < entries; i++ {
		k := i % differentKeys
		key := keys[k]

		e.put("put " + key + " " + key + key + key + key)

		//if i%5 == 0 && k%10 == 0 {
		//	e.delete("delete " + key)
		//}
		//fmt.Println(i)
	}
	//e.delete("delete " + strconv.Itoa(15))

}
