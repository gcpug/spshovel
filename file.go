package main

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

func ReadSQL(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	sql, err := ioutil.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(sql), nil
}

func Write(path string, records [][]string) (string, error) {
	fn := fmt.Sprintf("%s/%s.csv", path, time.Now().Format("20060102150405"))
	f, err := os.Create(fn)
	if err != nil {
		return "", err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if err := w.WriteAll(records); err != nil {
		return "", err
	}
	w.Flush()

	return fn, nil
}
