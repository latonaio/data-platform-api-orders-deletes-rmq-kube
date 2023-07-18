package requests

type Item struct {
	OrderID            		int     `json:"OrderID"`
	OrderItem          		int     `json:"OrderItem"`
	IsMarkedForDeletion     *bool   `json:"IsMarkedForDeletion"`
}
