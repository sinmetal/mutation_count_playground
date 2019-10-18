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

func TestInsert(t *testing.T) {
	ctx := context.Background()
	sc := createClient(ctx)

	empty := make(map[string]interface{})
	wihtIndex1 := map[string]interface{}{"withIndex1": ""}
	wihtIndex2 := map[string]interface{}{"withIndex2": ""}
	wihtIndexAll := map[string]interface{}{"withIndex1": "", "withIndex2": ""}

	cases := []struct {
		name              string
		normalColumnCount int
		addColumn         map[string]interface{}
		rowCount          int
		wantErr           bool
	}{
		// WithIndexをすべてNULLにした時、 [1:ID, 2:Arr1, 3:CreatedAt, 4:UpdatedAt, 5:CommitedAt, 6:MeasureWithIndex1_1, 7:MeasureWithIndex2_1, 8:MeasureWithIndex2_2] + normalColumnが 2 つで、10 になる
		{"empty : 2-2000", 2, empty, 2000, false},
		{"empty : 2-2001", 2, empty, 2001, true},

		// WithIndex1に値を入れて、WithIndex2をNULLにした時、 [1:ID, 2:Arr1, 3:CreatedAt, 4:UpdatedAt, 5:CommitedAt, 6:WithIndex1, 7:MeasureWithIndex1_1, 8:MeasureWithIndex2_1, 9:MeasureWithIndex2_2] + normalColumnが 1 つで、10 になる
		{"withIndex1 : 2-2000", 1, wihtIndex1, 2000, false},
		{"withIndex1 : 2-2001", 1, wihtIndex1, 2001, true},

		// WithIndex2に値を入れて、WithIndex1をNULLにした時、[1:ID, 2:Arr1, 3:CreatedAt, 4:UpdatedAt, 5:CommitedAt, 6:WithIndex2, 7:MeasureWithIndex1_1, 8:MeasureWithIndex2_1, 9:MeasureWithIndex2_2] + normalColumnが 1 つで、10 になる
		{"withIndex2 : 2-2000", 1, wihtIndex2, 2000, false},
		{"withIndex2 : 2-2001", 1, wihtIndex2, 2001, true},

		// WithIndex1とWitnIndex2に値を入れた時、 [1:ID, 2:Arr1, 3:CreatedAt, 4:UpdatedAt, 5:CommitedAt, 6:WithIndex1, 7:WithIndex2, 8:MeasureWithIndex1_1, 9:MeasureWithIndex2_1, 10:MeasureWithIndex2_2] + normalColumnが 0 つで、10 になる
		{"withIndexAll : 0-2000", 0, wihtIndexAll, 2000, false},
		{"withIndexAll : 0-2001", 0, wihtIndexAll, 2001, true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			mu, err := createInsertMutation(Table, tt.normalColumnCount, tt.addColumn, tt.rowCount)
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

// createInsertMutation is Insert Mutationを作成する
// normalColumnCount を指定することで、INDEXが付いていないカラムの数を調整する
// Measure TableはWithIndex1が1つ, WithIndex2が2つの合計3つのセカンダリインデックスを持ち、INSERT時はセカンダリインデックスを持つカラムがNULLの場合でも、
// Tableのカラムもセカンダリインデックスもmutationに含まれるので、mutation 数が +5 される
// 1:ID, 2:Arr1, 3:CreatedAt, 4:UpdatedAt, 5:CommitedAt, 6:WithIndex1, 7:WithIndex2
func createInsertMutation(table string, normalColumnCount int, addColumn map[string]interface{}, rowCount int) ([]*spanner.Mutation, error) {
	now := time.Now()
	list := make([]*spanner.Mutation, rowCount)
	for i := 0; i < rowCount; i++ {
		v := make(map[string]interface{})
		v["ID"] = uuid.New().String()
		for j := 1; j <= normalColumnCount; j++ {
			v[fmt.Sprintf("Col%d", j)] = ""
		}

		for addKey, addValue := range addColumn {
			v[addKey] = addValue
		}
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
