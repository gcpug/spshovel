package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/pkg/errors"
)

func ReadSQL(path string) (sql string, rerr error) {
	f, err := os.Open(path)
	if err != nil {
		return "", errors.WithStack(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			if rerr == nil {
				rerr = errors.WithStack(err)
				return
			}
			fmt.Printf("failed file.Close() err=%+v\n", err)
		}
	}()

	body, err := ioutil.ReadAll(f)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return string(body), nil
}

func NewCSVFile(path string) (string, *os.File, error) {
	fn := fmt.Sprintf("%s/%s.csv", path, time.Now().Format("20060102150405"))
	f, err := os.Create(fn)
	if err != nil {
		return "", nil, errors.WithStack(err)
	}
	return fn, f, nil
}
