package mutation_count_playground_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
)

const Table = "Measure"
const NoIndexTable = "MeasureNoIndex"

func TestInsertWithIndex1(t *testing.T) {
	ctx := context.Background()
	sc := createClient(ctx)

	cases := []struct {
		name              string
		normalColumnCount int
		rowCount          int
		wantErr           bool
	}{
		{"1-2000", 1, 2000, false},
		{"2-100", 2, 100, false},
		{"2-1000", 2, 1000, false},
		{"2-2000", 2, 2000, false},
		{"3-2000", 3, 2000, false},
		{"4-2000", 4, 2000, false},
		{"4-2001", 4, 2001, true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			mu, err := createInsertMutationWithIndex1(Table, tt.normalColumnCount, tt.rowCount)
			if err != nil {
				panic(err)
			}
			_, err = sc.Apply(ctx, mu)
			if tt.wantErr {
				if err == nil {
					t.Errorf("want err but got err is nil")
				} else if !strings.Contains(err.Error(), "The transaction contains too many mutations") {
					t.Errorf("error.err=%+v", err)
				}
			} else {
				if err != nil {
					t.Errorf("error.err=%+v", err)
				}
			}
		})
	}
}

// createInsertMutationWithIndex1 is WithIndex1カラムを含む Insert Mutationを作る
// normalColumnCount を指定することで、INDEXが付いていないカラムの数を調整する
func createInsertMutationWithIndex1(table string, normalColumnCount int, rowCount int) ([]*spanner.Mutation, error) {
	now := time.Now()
	list := make([]*spanner.Mutation, rowCount)
	for i := 0; i < rowCount; i++ {
		v := make(map[string]interface{})
		v["ID"] = uuid.New().String()
		for j := 1; j <= normalColumnCount; j++ {
			v[fmt.Sprintf("Col%d", j)] = ""
		}

		v["WithIndex1"] = "" // セカンダリインデックスを1つ持つカラム
		v["Arr1"] = []string{}
		v["CreatedAt"] = now
		v["UpdatedAt"] = now
		v["CommitedAt"] = spanner.CommitTimestamp
		list[i] = spanner.InsertMap(table, v)
	}

	return list, nil
}

func TestInsertNoIndexTable(t *testing.T) {
	ctx := context.Background()
	sc := createClient(ctx)

	cases := []struct {
		name    string
		count   int
		wantErr bool
	}{
		{"100", 100, false},
		{"1000", 1000, false},
		{"2000", 2000, false},
		{"2001", 2001, true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			mu, err := createInsertMutationColCount10(NoIndexTable, tt.count)
			if err != nil {
				panic(err)
			}
			_, err = sc.Apply(ctx, mu)
			if tt.wantErr {
				if err == nil {
					t.Errorf("want err but got err is nil")
				} else if !strings.Contains(err.Error(), "The transaction contains too many mutations") {
					t.Errorf("error.err=%+v", err)
				}
			} else {
				if err != nil {
					t.Errorf("error.err=%+v", err)
				}
			}
		})
	}
}

// createInsertMutationColCount10 is 10カラムが含まれるINSERT Mutationを作成する
func createInsertMutationColCount10(table string, count int) ([]*spanner.Mutation, error) {
	now := time.Now()
	list := make([]*spanner.Mutation, count)
	for i := 0; i < count; i++ {
		v := make(map[string]interface{})
		v["ID"] = uuid.New().String()
		v["Col1"] = ""
		v["Col2"] = ""
		v["Col3"] = ""
		v["Col4"] = ""
		v["Col5"] = ""
		v["Arr1"] = []string{}
		v["CreatedAt"] = now
		v["UpdatedAt"] = now
		v["CommitedAt"] = spanner.CommitTimestamp
		list[i] = spanner.InsertMap(table, v)
	}

	return list, nil
}

func createClient(ctx context.Context) *spanner.Client {
	config := spanner.ClientConfig{
		NumChannels: 12,
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened: 50,
		},
	}
	dataClient, err := spanner.NewClientWithConfig(ctx, "projects/gcpug-public-spanner/instances/merpay-sponsored-instance/databases/sinmetal", config)
	if err != nil {
		panic(err)
	}

	return dataClient
}
