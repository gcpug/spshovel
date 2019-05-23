package spanner_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	shovels "github.com/gcpug/spshovel/spanner"
	"github.com/google/uuid"
)

type SampleMutationer struct{}

var _ shovels.UpdateMutationer = &SampleMutationer{}

const TableName = "TweetHashKey"
const PrimaryKeyName = "Id"

func (m *SampleMutationer) GetKey(ctx context.Context, row *spanner.Row) (spanner.Key, error) {
	var id string
	if err := row.ColumnByName(PrimaryKeyName, &id); err != nil {
		return nil, err
	}
	return spanner.Key{id}, nil
}

func (m *SampleMutationer) GetMutation(ctx context.Context, row *spanner.Row) (*spanner.Mutation, error) {
	var id string
	if err := row.ColumnByName(PrimaryKeyName, &id); err != nil {
		return nil, err
	}

	return spanner.Update(TableName, []string{PrimaryKeyName, "Author"}, []interface{}{id, "sinmetal"}), nil
}

func TestSpannerEntityService_UpdateExperiment(t *testing.T) {
	ctx := context.Background()
	client := shovels.NewClient(ctx, "projects/gcpug-public-spanner/instances/merpay-sponsored-instance/databases/sinmetal")
	s := shovels.NewSpannerEntityService(client)
	count, err := s.UpdateExperiment(ctx, TableName, []string{PrimaryKeyName}, "SELECT Id From TweetHashKey", &SampleMutationer{})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("UPDATE Count : %d\n", count)
}

func TestDataInitialize(t *testing.T) {
	ctx := context.Background()
	client := shovels.NewClient(ctx, "projects/gcpug-public-spanner/instances/merpay-sponsored-instance/databases/sinmetal")
	var ml []*spanner.Mutation
	for i := 0; i < 1000000; i++ {
		ml = append(ml, spanner.Insert(TableName,
			[]string{PrimaryKeyName, "Author", "CommitedAt", "Content", "CreatedAt", "Favos", "Sort", "UpdatedAt"},
			[]interface{}{uuid.New().String(), "hoge", spanner.CommitTimestamp, "", time.Now(), []string{}, 0, time.Now()}))
		if i%1000 == 0 {
			_, err := client.Apply(ctx, ml)
			if err != nil {
				t.Fatal(err)
			}
			ml = []*spanner.Mutation{}
			fmt.Printf("INSERT COUNT : %d\n", i)
		}
	}

}
