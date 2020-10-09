package extensions

type Etl struct {
	//e  msgData
	Parameters [8]Parameter `json:"e"`
	Bt interface{}	`json:"bt"`
}

type source struct {
	u string
	n string
	sv string
}

type Parameter struct {
	Unit string `json:"u"`
	Name string `json:"n"`
	Value string `json:"v"`
}

type msgData struct {
	source
	parameters [7]Parameter
}

