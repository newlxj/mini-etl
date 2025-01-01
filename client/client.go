package main

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-log/log"
)

type RowData struct {
	Action     string        `json:"action"`
	TableName  string        `json:"table_name"`
	Columns    []string      `json:"columns"`
	Rows       []interface{} `json:"rows"`
	PrimaryKey []string      `json:"primary_key"`
}

type ClientConfig struct {
	TaskName    string `json:"taskName"`
	TaskAccount string `json:"taskAccount"`
	TaskDB      string `json:"taskDB"`
	TaskTable   string `json:"taskTable"`
	Client      struct {
		DBType     string `json:"dbType"`
		DBUrl      string `json:"dbUrl"`
		DBName     string `json:"dbName"`
		DBUsername string `json:"dbUsername"`
		DBPassword string `json:"dbPassword"`
	} `json:"client"`
}

func loadClientConfig(filename string) ([]ClientConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var configs []ClientConfig
	if err := json.Unmarshal(data, &configs); err != nil {
		return nil, err
	}
	return configs, nil
}

func main() {
	clientConfigs, err := loadClientConfig("clientConfig.json")
	if err != nil {
		log.Fatal(err)
	}
	if len(clientConfigs) == 0 {
		log.Fatal("No client configurations found")
	}
	clientConfig := clientConfigs[0]

	db, err := sql.Open("mysql", clientConfig.Client.DBUsername+":"+clientConfig.Client.DBPassword+"@tcp("+clientConfig.Client.DBUrl+")/"+clientConfig.Client.DBName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for {
		url := "http://127.0.0.1:8080/consume?taskAccount=" + clientConfig.TaskAccount + "&taskDB=" + clientConfig.TaskDB + "&taskTable=" + clientConfig.TaskTable
		resp, err := http.Get(url)
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

			switch rowData.Action {
			case "insert":
				columns := strings.Join(rowData.Columns, ", ")
				values := strings.Repeat("?, ", len(rowData.Columns))
				values = values[:len(values)-2] // Remove the trailing comma and space
				query := "INSERT INTO " + rowData.TableName + "_001 (" + columns + ") VALUES (" + values + ")"
				_, err = db.Exec(query, rowData.Rows...)
				if err != nil {
					log.Error(err)
				} else {
					log.Info("Data inserted successfully")
				}
			case "update":
				setClause := ""
				for _, col := range rowData.Columns {
					setClause += col + " = ?, "
				}
				setClause = setClause[:len(setClause)-2] // Remove the trailing comma and space

				whereClause := ""
				for _, pk := range rowData.PrimaryKey {
					whereClause += pk + " = ? AND "
				}
				whereClause = whereClause[:len(whereClause)-5] // Remove the trailing " AND "

				query := "UPDATE " + rowData.TableName + "_001 SET " + setClause + " WHERE " + whereClause

				// Split the rows into old and new values
				numColumns := len(rowData.Columns)
				oldValues := rowData.Rows[:numColumns]
				newValues := rowData.Rows[numColumns:]

				// Append the primary key values to the new values for the WHERE clause
				primaryKeyValues := oldValues[:len(rowData.PrimaryKey)]
				args := append(newValues, primaryKeyValues...)

				_, err = db.Exec(query, args...)
				if err != nil {
					log.Error(err)
				} else {
					log.Info("Data updated successfully")
				}
			case "delete":
				whereClause := ""
				for _, pk := range rowData.PrimaryKey {
					whereClause += pk + " = ? AND "
				}
				whereClause = whereClause[:len(whereClause)-5] // Remove the trailing " AND "

				query := "DELETE FROM " + rowData.TableName + "_001 WHERE " + whereClause
				_, err = db.Exec(query, rowData.Rows[:len(rowData.PrimaryKey)]...)
				if err != nil {
					log.Error(err)
				} else {
					log.Info("Data deleted successfully")
				}
			}
		}

		time.Sleep(2 * time.Second)
	}
}
