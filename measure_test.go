package mutation_count_playground_test

import (
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
	"github.com/google/uuid"
	"strings"
	"testing"
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
		// WithIndexをすべてNULLにした時、 [1:ID, 2:Arr1, 3:CommitedAt, 4:MeasureWithIndex1_1, 5:MeasureWithIndex2_1, 6:MeasureWithIndex2_2] + normalColumnが 4 つで、10 になる
		{"empty : 4-2000", 4, empty, 2000, false},
		{"empty : 4-2001", 4, empty, 2001, true},

		// WithIndex1に値を入れて、WithIndex2をNULLにした時、 [1:ID, 2:Arr1, 3:CommitedAt, 4:WithIndex1, 5:MeasureWithIndex1_1, 6:MeasureWithIndex2_1, 7:MeasureWithIndex2_2] + normalColumnが 3 つで、10 になる
		{"withIndex1 : 3-2000", 3, wihtIndex1, 2000, false},
		{"withIndex1 : 3-2001", 3, wihtIndex1, 2001, true},

		// WithIndex2に値を入れて、WithIndex1をNULLにした時、[1:ID, 2:Arr1, 3:CommitedAt, 4:WithIndex2, 5:MeasureWithIndex1_1, 6:MeasureWithIndex2_1, 7:MeasureWithIndex2_2] + normalColumnが 3 つで、10 になる
		{"withIndex2 : 3-2000", 3, wihtIndex2, 2000, false},
		{"withIndex2 : 3-2001", 3, wihtIndex2, 2001, true},

		// WithIndex1とWithIndex2に値を入れた時、 [1:ID, 2:Arr1, 3:CommitedAt, 4:WithIndex1, 5:WithIndex2, 6:MeasureWithIndex1_1, 7:MeasureWithIndex2_1, 8:MeasureWithIndex2_2] + normalColumnが 2 つで、10 になる
		{"withIndexAll : 2-2000", 2, wihtIndexAll, 2000, false},
		{"withIndexAll : 2-2001", 2, wihtIndexAll, 2001, true},
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
// Measure TableはWithIndex1が1つ, WithIndex2が2つの合計3つのセカンダリインデックスを持ち、INSERT時はセカンダリインデックスを持つカラムがNULLの場合も、セカンダリインデックスがmutationに含まれるので、mutation 数が +3 される
func createInsertMutation(table string, normalColumnCount int, addColumn map[string]interface{}, rowCount int) ([]*spanner.Mutation, error) {
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
	list := make([]*spanner.Mutation, count)
	for i := 0; i < count; i++ {
		v := make(map[string]interface{})
		v["ID"] = uuid.New().String()
		v["Col1"] = ""
		v["Col2"] = ""
		v["Col3"] = ""
		v["Col4"] = ""
		v["Col5"] = ""
		v["Col6"] = ""
		v["Col7"] = ""
		v["Arr1"] = []string{}
		v["CommitedAt"] = spanner.CommitTimestamp
		list[i] = spanner.InsertMap(table, v)
	}

	return list, nil
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	sc := createClient(ctx)

	empty := make(map[string]interface{})
	withIndex1 := map[string]interface{}{"withIndex1": ""}
	withIndex2 := map[string]interface{}{"withIndex2": ""}
	withIndexAll := map[string]interface{}{"withIndex1": "", "withIndex2": ""}

	cases := []struct {
		name              string
		normalColumnCount int
		updateColumn      map[string]interface{}
		rowCount          int
		wantErr           bool
	}{
		// WithIndexをすべてNULLにした時、 [1:ID, 2:Arr1, 3:CommitedAt] + normalColumnが 7 つで、10 になる
		{"empty : 7-2000", 7, empty, 2000, false},
		{"empty : 7-2001", 7, empty, 2001, true},

		// WithIndex1に値を入れて、WithIndex2をNULLにした時、 [1:ID, 2:Arr1, 3:CommitedAt, 4:WithIndex1, 5:MeasureWithIndex1_1, 6:?] + normalColumnが 4 つで、10 になる
		{"withIndex1 : 4-2000", 4, withIndex1, 2000, false},
		{"withIndex1 : 4-2001", 4, withIndex1, 2001, true},

		// WithIndex2に値を入れて、WithIndex1をNULLにした時、[1:ID, 2:Arr1, 3:CommitedAt, 4:WithIndex2, 5:MeasureWithIndex2_1, 6:MeasureWithIndex2_2, 7:?, 8:?] + normalColumnが 2 つで、10 になる
		{"withIndex2 : 2-2000", 2, withIndex2, 2000, false},
		{"withIndex2 : 2-2001", 2, withIndex2, 2001, true},

		// WithIndex1とWitnIndex2に値を入れた時、 [1:ID, 2:Arr1, 3:CommitedAt, 4:WithIndex1, 5:WithIndex2, 6:MeasureWithIndex1_1, 7:MeasureWithIndex2_1, 8:MeasureWithIndex2_2, 9:?, 10:?, 11:?] + normalColumnが 0 つで、11 になる
		{"withIndexAll : 0-1500", 0, withIndexAll, 1500, false},
		{"withIndexAll : 0-1600", 0, withIndexAll, 1600, false},
		{"withIndexAll : 0-1700", 0, withIndexAll, 1700, false},
		{"withIndexAll : 0-1818", 0, withIndexAll, 1818, false},
		{"withIndexAll : 0-1819", 0, withIndexAll, 1819, true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var ids []string
			{
				// UPDATEするために先にINSERTする
				var mu []*spanner.Mutation
				var err error
				ids, mu, err = createInsertMutationForUpdateTest(Table, tt.rowCount)
				if err != nil {
					panic(err)
				}
				_, err = sc.Apply(ctx, mu)
			}
			mu := createUpdateMutation(t, Table, ids, tt.normalColumnCount, tt.updateColumn, tt.rowCount)
			_, err := sc.Apply(ctx, mu)
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

// createInsertMutationForUpdateTest is Update のTestをする時に先にInsertするためのMutationを作る
// なるべくLimitに当たらないように更新mutationの数が小さくなるようにしておく
func createInsertMutationForUpdateTest(table string, rowCount int) ([]string, []*spanner.Mutation, error) {
	var ids = make([]string, rowCount)
	list := make([]*spanner.Mutation, rowCount)
	for i := 0; i < rowCount; i++ {
		id := uuid.New().String()
		ids[i] = id
		v := make(map[string]interface{})
		v["ID"] = id
		v["CommitedAt"] = spanner.CommitTimestamp
		list[i] = spanner.InsertMap(table, v)
	}

	return ids, list, nil
}

// createInsertMutation is Insert Mutationを作成する
// normalColumnCount を指定することで、INDEXが付いていないカラムの数を調整する
// Measure TableはWithIndex1が1つ, WithIndex2が2つの合計3つのセカンダリインデックスを持ち、INSERT時はセカンダリインデックスを持つカラムがNULLの場合も、セカンダリインデックスがmutationに含まれるので、mutation 数が +3 される
func createUpdateMutation(t *testing.T, table string, updateIDs []string, normalColumnCount int, updateColumn map[string]interface{}, rowCount int) []*spanner.Mutation {
	if len(updateIDs) != rowCount {
		t.Fatalf("updateIDs.length != rowCount !! updateIDs.length=%d, rowCount=%d", len(updateIDs), rowCount)
	}

	list := make([]*spanner.Mutation, rowCount)
	for i := 0; i < rowCount; i++ {
		v := make(map[string]interface{})
		v["ID"] = updateIDs[i]
		for j := 1; j <= normalColumnCount; j++ {
			v[fmt.Sprintf("Col%d", j)] = ""
		}

		for updateKey, updateValue := range updateColumn {
			v[updateKey] = updateValue
		}
		v["Arr1"] = []string{}
		v["CommitedAt"] = spanner.CommitTimestamp
		list[i] = spanner.UpdateMap(table, v)
	}

	return list
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
