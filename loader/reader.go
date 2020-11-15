package loader

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
)

// ReadPartion : Reading a partition
func ReadPartion(db *sql.DB, low string, high string, keyName string, tableName string, batchSize int, file *os.File, id int, wg *sync.WaitGroup) {

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s WHERE %s >= %s AND %s <= %s", tableName, keyName, low, keyName, high))
	defer wg.Done()
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	var csvRows strings.Builder
	numRows := 0

	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			panic(err)
		}
		data := make([]interface{}, 0)
		for range cols {
			data = append(data, new(string))
		}

		err = rows.Scan(data...)
		if err != nil {
			panic(err)
		}
		var csvRow strings.Builder
		for _, d := range data {
			csvRow.WriteString(*d.(*string) + ",")
		}
		csvRows.WriteString(csvRow.String() + "\n")
		numRows++
		if numRows == batchSize {
			file.WriteString(csvRows.String())
			csvRows.Reset()
		}
	}
	file.WriteString(csvRows.String())

	fmt.Printf("Parition %d Completed loading \n", id)
}
