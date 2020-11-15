package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	loader "github.com/ankushChatterjee/postgresimport/loader"

	_ "github.com/lib/pq"
)

func main() {

	tableName := flag.String("table", "public.table", "Fully Qualified table name")
	host := flag.String("host", "public.table", "Hostname")
	port := flag.Int("port", 5432, "Port of Postgres endpoint")
	keyRangesFull := flag.String("keyranges", "", "Key Ranges for paritioned read")
	targetFileName := flag.String("target", "target.csv", "Target file for loading from DB")
	user := flag.String("username", "postgres", "Username")
	password := flag.String("password", "postgres", "Password")
	dbname := flag.String("dbname", "postgres", "DB Name")
	keyName := flag.String("key", "id", "Parition Key")
	batchSize := flag.Int("batchSize", 100, "Read Batch Size")

	flag.Parse()

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		*host, *port, *user, *password, *dbname)

	db, err := sql.Open("postgres", psqlInfo)
	defer db.Close()
	if err != nil {
		panic(err)
	}

	keyRanges := strings.Split(*keyRangesFull, ",")

	targetFile, err := os.Create(*targetFileName)
	defer targetFile.Close()
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup

	tempDirPrefix := *host + "_" + *user + "_" + *tableName
	dirName, err := ioutil.TempDir("", tempDirPrefix)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dirName)

	numRanges := len(keyRanges)
	tempFiles := make([]*os.File, numRanges/2)
	for i := 0; i < numRanges; i += 2 {
		low := keyRanges[i]
		high := keyRanges[i+1]
		fName := *keyName + "_" + low + "_" + high
		filePath := filepath.Join(dirName, fName)
		fmt.Printf("Partition %d [%s]\n", i/2, filePath)
		file, err := os.Create(filePath)
		defer file.Close()
		if err != nil {
			panic(err)
		}
		tempFiles[i/2] = file
		wg.Add(1)
		go loader.ReadPartion(db, low, high, *keyName, *tableName, *batchSize, file, i/2, &wg)
		if err != nil {
			panic(err)
		}
	}

	wg.Wait()
	fmt.Println("All Paritions loaded : Merging temp files")
	for _, tf := range tempFiles {
		tfile, err := os.Open(tf.Name())
		defer tfile.Close()
		if err != nil {
			panic(err)
		}
		sc := bufio.NewScanner(tfile)
		for sc.Scan() {
			targetFile.WriteString(sc.Text() + "\n")
		}
		err = sc.Err()
		if err != nil {
			panic(err)
		}

	}
	fmt.Println("Merged files")

}
