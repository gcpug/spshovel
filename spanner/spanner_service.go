package spanner

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/gcpug/hake"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

type SpannerEntityService struct {
	sc *spanner.Client
}

func NewSpannerEntityService(sc *spanner.Client) *SpannerEntityService {
	return &SpannerEntityService{sc}
}

func (s *SpannerEntityService) QueryToWrite(ctx context.Context, sql string, header bool, w io.Writer) error {
	q := spanner.NewStatement(sql)

	cw := csv.NewWriter(w)
	hw := hake.NewWriter(cw, header)

	var count int
	tx, err := s.sc.BatchReadOnlyTransaction(ctx, spanner.ReadTimestamp(time.Now()))
	if err != nil {
		return errors.WithStack(err)
	}
	defer tx.Close()

	iter := tx.Query(ctx, q)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return errors.WithStack(err)
		}
		count++
		if err := hw.Write(row); err != nil {
			return errors.WithStack(err)
		}
		if count%1000 == 0 {
			cw.Flush()
		}
	}
	cw.Flush()

	return nil
}

type UpdateMutationer interface {
	GetKey(ctx context.Context, row *spanner.Row) (spanner.Key, error)
	GetMutation(ctx context.Context, row *spanner.Row) (*spanner.Mutation, error)
}

func (s *SpannerEntityService) UpdateExperiment(ctx context.Context, table string, columns []string, sql string, mutationer UpdateMutationer) (int, error) {
	q := spanner.NewStatement(sql)

	var rows []*spanner.Row

	var count int
	tx, err := s.sc.BatchReadOnlyTransaction(ctx, spanner.ReadTimestamp(time.Now()))
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer tx.Close()

	iter := tx.Query(ctx, q)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, errors.WithStack(err)
		}
		count++
		rows = append(rows, row)

		if count%500 == 0 {
			if err := s.update(ctx, table, columns, rows, mutationer); err != nil {
				return 0, errors.WithStack(err)
			}
			rows = []*spanner.Row{}
		}
	}
	if len(rows) > 0 {
		if err := s.update(ctx, table, columns, rows, mutationer); err != nil {
			return 0, errors.WithStack(err)
		}
	}

	return count, nil
}

func (s *SpannerEntityService) update(ctx context.Context, table string, columns []string, rows []*spanner.Row, mutationer UpdateMutationer) error {
	var keys spanner.KeySet
	for _, v := range rows {
		key, err := mutationer.GetKey(ctx, v)
		if err != nil {
			return errors.WithStack(err)
		}
		if key == nil {
			// TODO 専用のerrorを作る sinmetal
			return fmt.Errorf("key is required")
		}
		keys = append(key)
	}

	_, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		var ml []*spanner.Mutation

		iter := txn.Read(ctx, table, keys, columns)
		defer iter.Stop()
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return errors.WithStack(err)
			}
			m, err := mutationer.GetMutation(ctx, row)
			if err != nil {
				return errors.WithStack(err)
			}
			ml = append(ml, m)
		}

		return txn.BufferWrite(ml)
	})
	if err != nil {
		return err
	}

	return nil
}
