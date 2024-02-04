package Engine

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
)

func checkInput(input string) int {
	getRegex := regexp.MustCompile(GETREGEX)
	deleteRegex := regexp.MustCompile(DELETEREGEX)
	putRegex := regexp.MustCompile(PUTREGEX)
	exitRegex := regexp.MustCompile(EXITREGEX)
	makeRegex := regexp.MustCompile(MAKEREGEX)
	destroyRegex := regexp.MustCompile(DESTROYREGEX)
	fingerprintRegex := regexp.MustCompile(FINGERPRINTREGEX)
	addToStructRegex := regexp.MustCompile(ADDTOSTRUCTREGEX)
	checkRegex := regexp.MustCompile(CHECKSTRUCTURE)
	checkHllRegex := regexp.MustCompile(CHECKSTRUCTUREHLL)
	simhashRegex := regexp.MustCompile(SIMHASHREGEX)
	prefixscanRegex := regexp.MustCompile(PREFIXSCANREGEX)
	rangescanRegex := regexp.MustCompile(RANGESCANREGEX)
	prefixiterRegex := regexp.MustCompile(PREFIXITERREGEX)
	rangeiterRegex := regexp.MustCompile(RANGEITERREGEX)

	if getRegex.MatchString(input) {
		return OPTION_GET
	} else if deleteRegex.MatchString(input) {
		return OPTION_DELETE
	} else if putRegex.MatchString(input) {
		return OPTION_PUT
	} else if exitRegex.MatchString(input) {
		return OPTION_EXIT
	} else if makeRegex.MatchString(input) {
		return OPTION_MAKE
	} else if destroyRegex.MatchString(input) {
		return OPTION_DESTROY
	} else if fingerprintRegex.MatchString(input) {
		return OPTION_FINGERPRINT
	} else if addToStructRegex.MatchString(input) {
		return OPTION_ADDTOSTRUCT
	} else if checkRegex.MatchString(input) || checkHllRegex.MatchString(input) {
		return OPTION_CHECKSTRUCT
	} else if simhashRegex.MatchString(input) {
		return OPTION_SIMHASH
	} else if prefixscanRegex.MatchString(input) {
		return OPTION_PREFIXSCAN
	} else if rangescanRegex.MatchString(input) {
		return OPTION_RANGESCAN
	} else if prefixiterRegex.MatchString(input) {
		return OPTION_PREFIXITER
	} else if rangeiterRegex.MatchString(input) {
		return OPTION_RANGEITER
	} else {
		return OPTION_INVALID
	}
}

func getInput() string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(PROMPTCAHAR)
	input, _ := reader.ReadString('\n')
	input = strings.TrimRight(input, "\n")
	input = strings.TrimRight(input, "\r")

	return input
}

func showValidOptions() {
	fmt.Println("---------------------------------------------------------------------------------------------------")
	fmt.Println("You must use one of the following commands:")
	fmt.Println()
	fmt.Println("exit -> exits program")
	fmt.Println("get {key} -> gets the value")
	fmt.Println("put {key} {value} -> stores the key-value pair")
	fmt.Println()
	fmt.Println("(bf|cms|hll) make {name} -> makes structrue")
	fmt.Println("(bf|cms|hll) destroy {name} -> destroys structure")
	fmt.Println("(bf|cms|hll) put {name} {data} -> adds data to structure")
	fmt.Println("(bf|cms|hll) check {name} {data} -> checks the structure by data")
	fmt.Println()
	fmt.Println("fingerprint {name} {text} -> saves text fingerprint")
	fmt.Println("simhash {fingerprint_name} {fingerprint_name} -> calculates simhash for fingerprints")
	fmt.Println()
	fmt.Println("prefixscan {prefix} {page} {page_size} -> does prefix scann")
	fmt.Println("rangescan {rangeMin}-{rangeMax} {page} {page_size} -> does range scann")
	fmt.Println()
	fmt.Println("prefixiterate {prefix} -> enters prefix iterator")
	fmt.Println("rangeiterate {rangeMin}-{rangeMax} -> enters range iterator")
	fmt.Println("next -> gets nex element when in iterator mode")
	fmt.Println("stop -> stop exits iterator mode")
	fmt.Println("---------------------------------------------------------------------------------------------------")
}

func pauseTerminal() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("PRESS ENTER")
	_, _ = reader.ReadString('\n')
}

func displayError(err error) {
	fmt.Println("---------------------------------------------------------------------------------------------------")
	fmt.Print("ERROR : ")
	fmt.Println(err)
	fmt.Println("---------------------------------------------------------------------------------------------------")
}
