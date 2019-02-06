package spanner

import (
	"context"
	"log"

	"cloud.google.com/go/spanner"
)

func NewClient(ctx context.Context, db string) *spanner.Client {
	dataClient, err := spanner.NewClient(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	return dataClient
}
