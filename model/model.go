package model

type Cars struct {
	Cars []Car `json:"cars"`
}

type Car struct {
	Number string `json:"number"`
	Model string `json:"model"`
	Year int `json:"year"`
	Mileage int `json:"mileage"`
	InspectionDate string `json:"inspection_date"`
	Color string `json:"color"`

}