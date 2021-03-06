package spanner

import (
	"context"
	"encoding/csv"
	"io"

	"cloud.google.com/go/spanner"
	"github.com/gcpug/hake"
	"github.com/morikuni/failure"
	"google.golang.org/api/iterator"
)

type SpannerEntityService struct {
	sc *spanner.Client
}

func NewSpannerEntityService(sc *spanner.Client) *SpannerEntityService {
	return &SpannerEntityService{sc}
}

func (s *SpannerEntityService) QueryDump(ctx context.Context, sql string, header bool, w io.Writer) error {
	q := spanner.NewStatement(sql)

	cw := csv.NewWriter(w)
	hw := hake.NewWriter(cw, header)

	var count int
	iter := s.sc.Single().Query(ctx, q)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return failure.Wrap(err, failure.WithCode(SpannerInternalError))
		}
		count++
		if err := hw.Write(row); err != nil {
			return failure.Wrap(err, failure.WithCode(HakeInternalError))
		}
		if count%1000 == 0 {
			cw.Flush()
		}
	}
	cw.Flush()

	return nil
}
