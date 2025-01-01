package main

import (
	"encoding/json"
	"net/http"
	"os"
	"sync"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/siddontang/go-log/log"
)

type MyEventHandler struct {
	canal.DummyEventHandler
	file *os.File
	mu   sync.Mutex
}

type RowData struct {
	Action    string        `json:"action"`
	TableName string        `json:"table_name"`
	Columns   []string      `json:"columns"`
	Rows      []interface{} `json:"rows"`
}

func flattenRows(rows [][]interface{}) []interface{} {
	var flatRows []interface{}
	for _, row := range rows {
		flatRows = append(flatRows, row...)
	}
	return flatRows
}

func NewMyEventHandler(filename string) (*MyEventHandler, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &MyEventHandler{file: file}, nil
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	rowData := RowData{
		Action:    e.Action,
		TableName: e.Table.Name,
		Columns:   getColumnNames(e.Table),
		Rows:      flattenRows(e.Rows),
	}
	data, err := json.Marshal(rowData)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := h.file.Write(data); err != nil {
		return err
	}
	log.Infof("%s", string(data))
	return nil
}
func getColumnNames(table *schema.Table) []string {
	var columnNames []string
	for _, col := range table.Columns {
		columnNames = append(columnNames, col.Name)
	}
	return columnNames
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

func (h *MyEventHandler) ReadAndClear() (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.file.Close()

	data, err := os.ReadFile("abc.txt")
	if err != nil {
		return "", err
	}

	if err := os.WriteFile("abc.txt", []byte{}, 0644); err != nil {
		return "", err
	}

	h.file, err = os.OpenFile("abc.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func main() {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = "127.0.0.1:3306"
	cfg.User = "root"
	cfg.Password = "123456"
	cfg.Dump.TableDB = "chat"
	cfg.Dump.Tables = []string{"users"}
	cfg.Dump.ExecutionPath = "" // Disable mysqldump

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	handler, err := NewMyEventHandler("abc.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer handler.file.Close()

	c.SetEventHandler(handler)

	go func() {
		if err := c.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	http.HandleFunc("/consume", func(w http.ResponseWriter, r *http.Request) {
		data, err := handler.ReadAndClear()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte(data))
	})

	log.Info("Server is running on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
