package spanner

import (
	"context"
	"log"

	"cloud.google.com/go/spanner"
	"github.com/morikuni/failure"
)

var SpannerInternalError failure.StringCode = "SpannerInternalError"
var HakeInternalError failure.StringCode = "HakeInternalError"

func NewClient(ctx context.Context, db string) *spanner.Client {
	dataClient, err := spanner.NewClient(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	return dataClient
}
