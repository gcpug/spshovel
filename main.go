package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/gcpug/spshovel/spanner"
)

type Param struct {
	Project     string
	Instance    string
	Database    string
	SqlFilePath string
	NoHeader    bool
}

func main() {
	param, err := getFlag()
	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}

	db := fmt.Sprintf("projects/%s/instances/%s/databases/%s", param.Project, param.Instance, param.Database)
	fmt.Println(db)

	wd, err := os.Getwd()
	if err != nil {
		fmt.Printf("failed get working dir. err=%+v\n", err)
	}

	sql, err := ReadSQL(param.SqlFilePath)
	if err != nil {
		fmt.Printf("failed read sql file. err=%+v\n", err)
		os.Exit(1)
	}
	fmt.Println(sql)
	fmt.Println()

	ctx := context.Background()
	sc := spanner.NewClient(ctx, db)
	s := spanner.NewSpannerEntityService(sc)
	cn, data, err := s.Query(ctx, sql)
	if err != nil {
		fmt.Printf("failed query to spanner. err=%+v\n", err)
	}

	if !param.NoHeader {
		data, data[0] = append(data[0:1], data[0:]...), cn
	}

	fn, err := Write(wd, data)
	if err != nil {
		fmt.Printf("failed write file. err=%+v\n", err)
	}
	fmt.Printf("output %s !", fn)
}

func getFlag() (*Param, error) {
	var (
		project     = flag.String("project", "", "project is spanner project")
		instance    = flag.String("instance", "", "instance is spanner insntace")
		database    = flag.String("database", "", "database is spanner database")
		sqlFilePath = flag.String("sql-file-path", "", "sql-file-path is sql file path")
		noHeader    = flag.Bool("no-header", false, "csv header not output")
	)
	flag.Parse()

	var emsg string
	if len(*project) < 1 {
		emsg += "project is required\n"
	}
	if len(*instance) < 1 {
		emsg += "instance is required\n"
	}
	if len(*database) < 1 {
		emsg += "database is required\n"
	}
	if len(*sqlFilePath) < 1 {
		emsg += "sql-file-path is required\n"
	}

	if len(emsg) > 0 {
		return nil, errors.New(emsg)
	}

	return &Param{
		Project:     *project,
		Instance:    *instance,
		Database:    *database,
		SqlFilePath: *sqlFilePath,
		NoHeader:    *noHeader,
	}, nil
}
