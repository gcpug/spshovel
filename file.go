package main

import (
	"fmt"
	"github.com/pkg/errors"
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

func NewCSVFile(path string) (string, *os.File, error) {
	fn := fmt.Sprintf("%s/%s.csv", path, time.Now().Format("20060102150405"))
	f, err := os.Create(fn)
	if err != nil {
		return "", nil, errors.WithStack(err)
	}
	return fn, f, nil
}
