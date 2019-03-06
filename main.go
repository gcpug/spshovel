package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/gcpug/spshovel/spanner"
	"github.com/pkg/errors"
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
	fn, err := run(ctx, param, db, sql, wd)
	if err != nil {
		fmt.Printf("+%v\n", err)
		os.Exit(1)
	}
	fmt.Printf("output %s", fn)
}

func run(ctx context.Context, param *Param, db string, sql string, output string) (fileName string, rerr error) {
	sc := spanner.NewClient(ctx, db)
	s := spanner.NewSpannerEntityService(sc)
	fn, f, err := NewCSVFile(output)
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
	if err := s.QueryToWrite(ctx, sql, !param.NoHeader, f); err != nil {
		return "", errors.WithMessage(err, "failed query to spanner with output file.")
	}

	return fn, nil
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
