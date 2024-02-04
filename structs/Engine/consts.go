package Engine

const (
	PROMPTCAHAR = ">"

	OPTION_EXIT    = -1
	OPTION_INVALID = 0

	OPTION_PUT    = 1
	OPTION_GET    = 2
	OPTION_DELETE = 3

	OPTION_MAKE        = 4
	OPTION_DESTROY     = 5
	OPTION_ADDTOSTRUCT = 6
	OPTION_CHECKSTRUCT = 7

	OPTION_FINGERPRINT = 8
	OPTION_SIMHASH     = 9

	OPTION_PREFIXSCAN = 10
	OPTION_RANGESCAN  = 11

	OPTION_PREFIXITER = 12
	OPTION_RANGEITER  = 13

	EXITREGEX = `^exit$`

	PUTREGEX    = `^put\s\w+\s.+$`
	GETREGEX    = `^get\s\w+$`
	DELETEREGEX = `^delete\s\w+$`

	MAKEREGEX         = `^(bf|cms|hll) make \w+$`
	DESTROYREGEX      = `^(bf|cms|hll) destroy \w+$`
	ADDTOSTRUCTREGEX  = `^(bf|cms|hll) put \w+ .+$`
	CHECKSTRUCTURE    = `^(bf|cms) check \w+ .+$`
	CHECKSTRUCTUREHLL = `^hll check \w+$`

	FINGERPRINTREGEX = `^fingerprint \w+ .+$`
	SIMHASHREGEX     = `^simhash \w+ \w+$`

	PREFIXSCANREGEX = `^prefixscan \w+ \d+ \d+$`
	RANGESCANREGEX  = `^rangescan \w+-\w+ \d+ \d+$`

	PREFIXITERREGEX = `^prefixiterate \w+$`
	RANGEITERREGEX  = `^rangeiterate \w+-\w+$`

	STOPREGEX = `^stop$`
	NEXTREGEX = `^next$`

	SYSTEMKEY = `(hll|cms|fingerprint|tokenLog|bf|)\s.+$`
)
