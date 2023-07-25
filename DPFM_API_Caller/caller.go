package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-orders-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-orders-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-orders-deletes-rmq-kube/config"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
	"golang.org/x/xerrors"
)

type DPFMAPICaller struct {
	ctx  context.Context
	conf *config.Conf
	rmq  *rabbitmq.RabbitmqClient
	db   *database.Mysql
}

func NewDPFMAPICaller(
	conf *config.Conf, rmq *rabbitmq.RabbitmqClient, db *database.Mysql,
) *DPFMAPICaller {
	return &DPFMAPICaller{
		ctx:  context.Background(),
		conf: conf,
		rmq:  rmq,
		db:   db,
	}
}

func (c *DPFMAPICaller) AsyncDeletes(
	accepter []string,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (interface{}, []error) {
	var response interface{}
	switch input.APIType {
	case "deletes":
		response = c.deleteSqlProcess(input, output, accepter, log)
	default:
		log.Error("unknown api type %s", input.APIType)
	}
	return response, nil
}

func (c *DPFMAPICaller) deleteSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	log *logger.Logger,
) *dpfm_api_output_formatter.Message {
	var headerData *dpfm_api_output_formatter.Header
	itemData := make([]dpfm_api_output_formatter.Item, 0)
	itemScheduleLineData := make([]dpfm_api_output_formatter.ItemScheduleLine, 0)
	for _, a := range accepter {
		switch a {
		case "Header":
			h, i, s := c.headerDelete(input, output, log)
			headerData = h
			if h == nil || i == nil || s == nil {
				continue
			}
			itemData = append(itemData, *i...)
			itemScheduleLineData = append(itemScheduleLineData, *s...)
		case "Item":
			i, s := c.itemDelete(input, output, log)
			if i == nil || s == nil {
				continue
			}
			itemData = append(itemData, *i...)
			itemScheduleLineData = append(itemScheduleLineData, *s...)
		case "Schedule":
			s := c.itemScheduleLineDelete(input, output, log)
			if s == nil {
				continue
			}
			itemScheduleLineData = append(itemScheduleLineData, *s...)
		}
	}

	return &dpfm_api_output_formatter.Message{
		Header:       		headerData,
		Item:         		&itemData,
		ItemScheduleLine:	&itemScheduleLineData,
	}
}

func (c *DPFMAPICaller) headerDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.Header, *[]dpfm_api_output_formatter.Item, *[]dpfm_api_output_formatter.ItemScheduleLine) {
	sessionID := input.RuntimeSessionID

	header := c.HeaderDelete(input, log)
	if header == nil {
		return nil, nil, nil
	}
	header.IsMarkedForDeletion = input.Header.IsMarkedForDeletion
	res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "OrdersHeader", "runtime_session_id": sessionID})
	if err != nil {
		err = xerrors.Errorf("rmq error: %w", err)
		log.Error("%+v", err)
		return nil, nil, nil
	}
	res.Success()
	if !checkResult(res) {
		output.SQLUpdateResult = getBoolPtr(false)
		output.SQLUpdateError = "Header Data cannot delete"
		return nil, nil, nil
	}

	// headerの削除が取り消された時は子に影響を与えない
	if !*header.IsMarkedForDeletion {
		return header, nil, nil
	}

	items := c.ItemsDelete(input, log)
	for i := range *items {
		(*items)[i].IsMarkedForDeletion = input.Header.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*items)[i], "function": "OrdersItem", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Item Data cannot delete"
			return nil, nil, nil
		}
	}

	itemScheduleLines := c.ItemScheduleLineDelete(input, log)
	for i := range *itemScheduleLines {
		(*itemScheduleLines)[i].IsMarkedForDeletion = input.Header.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*itemScheduleLines)[i], "function": "OrdersItemScheduleLine", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Item Schedule Line Data cannot delete"
			return nil, nil, nil
		}
	}

	return header, items, itemScheduleLines
}

func (c *DPFMAPICaller) itemDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.Item, *[]dpfm_api_output_formatter.ItemScheduleLine) {
	sessionID := input.RuntimeSessionID
	itemScheduleLines := c.ItemScheduleLineDelete(input, log)
	item := input.Header.Item[0]
	for _, v := range *itemScheduleLines {
		v.IsMarkedForDeletion = item.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": v, "function": "OrdersItemScheduleLine", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Item Schedule Line Data cannot delete"
			return nil, nil
		}
	}

	item := make([]dpfm_api_output_formatter.Item, 0)
	for _, v := range input.Orders.Item {
		data := dpfm_api_output_formatter.Item{
			OrderID:            	input.Header.OrderID,
			OrderItem:          	v.OrderItem,
			ItemDeliveryStatus: 	nil,
			IsMarkedForDeletion:    v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "OrdersItem", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Order Item Data cannot delete"
			return nil, nil
		}
	}

	// itemがキャンセル取り消しされた場合、headerのキャンセルも取り消す
	if !*input.Header.Item[0].IsMarkedForDeletion {
		header := c.HeaderDelete(input, log)
		header.HeaderIsDeleted = input.Header.Item[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "OrdersHeader", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Header Data cannot delete"
			return nil, nil
		}
	}

	return &item, itemScheduleLines
}

func (c *DPFMAPICaller) itemScheduleDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.ItemScheduleLine {
	sessionID := input.RuntimeSessionID
	itemScheduleLines := make([]dpfm_api_output_formatter.ItemScheduleLine, 0)
	for _, item := range input.Orders.Item {
		for _, itemScheduleLine := range item.ItemSchedulingLine {
			data := dpfm_api_output_formatter.ScheduleLine{
				OrderID:             input.Orders.OrderID,
				OrderItem:           item.OrderItem,
				ScheduleLine:        itemScheduleLine.ScheduleLine,
				IsMarkedForDeletion: itemScheduleLine.IsMarkedForDeletion,
			}
			res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "OrdersItemScheduleLine", "runtime_session_id": sessionID})
			if err != nil {
				err = xerrors.Errorf("rmq error: %w", err)
				log.Error("%+v", err)
				return nil
			}
			res.Success()
			if !checkResult(res) {
				output.SQLUpdateResult = getBoolPtr(false)
				output.SQLUpdateError = "Item Schedule Line Data cannot delete"
				return nil
			}
			schedules = append(itemScheduleLines, data)
		}
	}
	return &itemScheduleLines
}

func checkResult(msg rabbitmq.RabbitmqMessage) bool {
	data := msg.Data()
	d, ok := data["result"]
	if !ok {
		return false
	}
	result, ok := d.(string)
	if !ok {
		return false
	}
	return result == "success"
}

func getBoolPtr(b bool) *bool {
	return &b
}
