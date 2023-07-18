package requests

type ItemScheduleLine struct {
	OrderID             int   `json:"OrderID"`
	OrderItem           int   `json:"OrderItem"`
	ScheduleLine        int   `json:"ScheduleLine"`
	IsMarkedForDeletion *bool `json:"IsMarkedForDeletion"`
}
