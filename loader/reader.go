package loader

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"sync"
)

// LoadPartition : Reading a partition
func LoadPartition(db *sql.DB, low string, high string, keyName string, tableName string, batchSize int, file *os.File, id int, wg *sync.WaitGroup) {

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s WHERE %s >= %s AND %s <= %s", tableName, keyName, low, keyName, high))
	defer wg.Done()
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	csvRows := make([][]string, 0)
	numRows := 0
	csvWriter := csv.NewWriter(file)
	defer csvWriter.Flush()

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
		dataStr := make([]string, 0)
		for _, d := range data {
			dataStr = append(dataStr, *d.(*string))
		}

		csvRows = append(csvRows, dataStr)
		numRows++
		if numRows == batchSize {
			csvWriter.WriteAll(csvRows)
			csvWriter.Flush()
			csvRows = make([][]string, 0)
		}
	}
	csvWriter.WriteAll(csvRows)
	csvWriter.Flush()
	csvRows = make([][]string, 0)
	fmt.Printf("Parition %d Completed loading \n", id)
}
