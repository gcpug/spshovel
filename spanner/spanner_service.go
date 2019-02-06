package spanner

import (
	"context"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
	"github.com/sinmetal/hake"
	"google.golang.org/api/iterator"
)

type SpannerEntityService struct {
	sc *spanner.Client
}

func NewSpannerEntityService(sc *spanner.Client) *SpannerEntityService {
	return &SpannerEntityService{sc}
}

func (s *SpannerEntityService) Query(ctx context.Context, sql string) ([]string, [][]string, error) {
	q := spanner.NewStatement(sql)

	var columnNames []string
	var rows [][]string
	iter := s.sc.Single().Query(ctx, q)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if len(columnNames) < 1 {
			columnNames = row.ColumnNames()
		}

		a, err := (*hake.Row)(row).ToStringArray()
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		rows = append(rows, a)
	}

	return columnNames, rows, nil
}
