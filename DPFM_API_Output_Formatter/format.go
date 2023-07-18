package dpfm_api_output_formatter

import (
	"database/sql"
	"fmt"
)

func ConvertToHeader(rows *sql.Rows) (*[]Header, error) {
	defer rows.Close()
	header := Header{}
	i := 0

	for rows.Next() {
		i++
		err := rows.Scan(
			&header.OrderID,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &header, err
		}
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return nil, nil
	}

	return &header, nil
}

func ConvertToItem(rows *sql.Rows) (*[]Item, error) {
	defer rows.Close()
	items := make([]Item, 0)
	i := 0

	for rows.Next() {
		i++
		item := Item{}
		err := rows.Scan(
			&item.OrderID,
			&item.OrderItem,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &items, err
		}

		items = append(items, item)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &items, nil
	}

	return &items, nil
}

func ConvertToItemScheduleLine(rows *sql.Rows) (*[]ItemScheduleLine, error) {
	defer rows.Close()
	itemSchedulelines := make([]ItemScheduleLine, 0)
	i := 0

	for rows.Next() {
		i++
		itemScheduleLine := ItemScheduleLine{}
		err := rows.Scan(
			&itemScheduleLine.OrderID,
			&itemScheduleLine.OrderItem,
			&itemScheduleLine.ScheduleLine,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &itemSchedulelines, err
		}

		itemSchedulelines = append(itemSchedulelines, itemScheduleLine)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &scitemSchedulelineshedules, nil
	}

	return &itemSchedulelines, nil
}
