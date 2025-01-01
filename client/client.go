package main

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-log/log"
)

type RowData struct {
	Action    string        `json:"action"`
	TableName string        `json:"table_name"`
	Columns   []string      `json:"columns"`
	Rows      []interface{} `json:"rows"`
}

func main() {
	db, err := sql.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/chat")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for {
		resp, err := http.Get("http://127.0.0.1:8080/consume")
		if err != nil {
			log.Error(err)
			time.Sleep(2 * time.Second)
			continue
		}

		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Error(err)
			time.Sleep(2 * time.Second)
			continue
		}

		if len(data) > 0 {
			var rowData RowData
			if err := json.Unmarshal(data, &rowData); err != nil {
				log.Error(err)
				time.Sleep(2 * time.Second)
				continue
			}

			if rowData.Action == "insert" {
				columns := strings.Join(rowData.Columns, ", ")
				values := strings.Repeat("?, ", len(rowData.Columns))
				values = values[:len(values)-2] // Remove the trailing comma and space
				// query := "INSERT INTO " + rowData.TableName + " (" + columns + ") VALUES (" + values + ")"
				query := "INSERT INTO " + rowData.TableName + "_001 (" + columns + ") VALUES (" + values + ")"
				_, err = db.Exec(query, rowData.Rows...)
				if err != nil {
					log.Error(err)
				} else {
					log.Info("Data inserted successfully")
				}
			}
		}

		time.Sleep(2 * time.Second)
	}
}
