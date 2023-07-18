package requests

type Header struct {
	OrderID              	 int     `json:"OrderID"`
	IsMarkedForDeletion      *bool   `json:"IsMarkedForDeletion"`
}
