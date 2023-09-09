package requests

type Header struct {
	OrderID              	 int     `json:"OrderID"`
	HeaderDeliveryStatus	 *string `json:"HeaderDeliveryStatus"`
	IsMarkedForDeletion      *bool   `json:"IsMarkedForDeletion"`
}
