package main

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/siddontang/go-log/log"
)

type MyEventHandler struct {
	canal.DummyEventHandler
	file *os.File
	mu   sync.Mutex
}

type RowData struct {
	Action     string        `json:"action"`
	TableName  string        `json:"table_name"`
	Columns    []string      `json:"columns"`
	Rows       []interface{} `json:"rows"`
	PrimaryKey []string      `json:"primary_key"`
}

type BinlogPosition struct {
	Name string `json:"name"`
	Pos  uint32 `json:"pos"`
}

type ServerConfig struct {
	DBType     string   `json:"dbType"`
	DBUrl      string   `json:"dbUrl"`
	DBUsername string   `json:"dbUsername"`
	DBPassword string   `json:"dbPassword"`
	DBDBs      []string `json:"dbDBs"`
	DataDelete bool     `json:"dataDelete"`
	DataUpdate bool     `json:"dataUpdate"`
	DataInsert bool     `json:"dataInsert"`
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
		Action:     e.Action,
		TableName:  e.Table.Name,
		Columns:    getColumnNames(e.Table),
		Rows:       flattenRows(e.Rows),
		PrimaryKey: getPrimaryKeyColumns(e.Table),
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

func getPrimaryKeyColumns(table *schema.Table) []string {
	var primaryKeyColumns []string
	for _, col := range table.Columns {
		if table.IsPrimaryKey(table.FindColumn(col.Name)) {
			primaryKeyColumns = append(primaryKeyColumns, col.Name)
		}
	}
	return primaryKeyColumns
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

func (h *MyEventHandler) ReadAndClear(filename string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.file.Close()

	data, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}

	if err := os.WriteFile(filename, []byte{}, 0644); err != nil {
		return "", err
	}

	h.file, err = os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func saveBinlogPosition(pos mysql.Position, filename string) error {
	data, err := json.Marshal(BinlogPosition{Name: pos.Name, Pos: pos.Pos})
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

func loadBinlogPosition(filename string) (mysql.Position, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return mysql.Position{}, nil
		}
		return mysql.Position{}, err
	}
	var pos BinlogPosition
	if err := json.Unmarshal(data, &pos); err != nil {
		return mysql.Position{}, err
	}
	return mysql.Position{Name: pos.Name, Pos: pos.Pos}, nil
}

func (h *MyEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, gtid mysql.GTIDSet, force bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	filename := h.file.Name()[:strings.LastIndex(h.file.Name(), "-")] + "-binlog_position.json"
	if err := saveBinlogPosition(pos, filename); err != nil {
		return err
	}
	return nil
}

func loadServerConfig(filename string) ([]ServerConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var configs []ServerConfig
	if err := json.Unmarshal(data, &configs); err != nil {
		return nil, err
	}
	return configs, nil
}

func main() {
	serverConfigs, err := loadServerConfig("serverConfig.json")
	if err != nil {
		log.Fatal(err)
	}
	if len(serverConfigs) == 0 {
		log.Fatal("No server configurations found")
	}
	cfg := canal.NewDefaultConfig()
	cfg.Addr = serverConfigs[0].DBUrl
	cfg.User = serverConfigs[0].DBUsername
	cfg.Password = serverConfigs[0].DBPassword
	cfg.Dump.TableDB = serverConfigs[0].DBDBs[0]
	cfg.Dump.Tables = []string{"users"} // This should be dynamically set based on the request
	cfg.Dump.ExecutionPath = ""         // Disable mysqldump

	filename := cfg.User + "-" + cfg.Dump.TableDB + "-" + cfg.Dump.Tables[0] + ".blog"
	handler, err := NewMyEventHandler(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer handler.file.Close()

	// Start the HTTP server in a separate goroutine
	go func() {
		http.HandleFunc("/consume", func(w http.ResponseWriter, r *http.Request) {
			taskAccount := r.URL.Query().Get("taskAccount")
			taskDB := r.URL.Query().Get("taskDB")
			taskTable := r.URL.Query().Get("taskTable")
			filename := taskAccount + "-" + taskDB + "-" + taskTable + ".blog"
			data, err := handler.ReadAndClear(filename)
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
	}()

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	c.SetEventHandler(handler)

	posFilename := cfg.User + "-binlog_position.json"
	pos, err := loadBinlogPosition(posFilename)
	if err != nil {
		log.Fatal(err)
	}
	if pos.Name != "" {
		c.RunFrom(pos)
	}

	if err := c.Run(); err != nil {
		log.Fatal(err)
	}
}
