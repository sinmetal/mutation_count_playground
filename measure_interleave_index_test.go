package mutation_count_playground_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
)

const InterleaveParentWithIndexTable = "MeasureParentWithIndex"
const InterleaveChildWithIndexTable = "MeasureChildWithIndex"

func TestMeasureInterleaveWithIndex_Insert(t *testing.T) {
	ctx := context.Background()
	sc := createClient(ctx, t)

	empty := make(map[string]interface{})

	cases := []struct {
		name              string
		normalColumnCount int
		addColumn         map[string]interface{}
		rowCount          int
		wantErr           bool
	}{
		// Parent: [1:ID, 2:Arr1, 3:CommitedAt] + normalColumnが 7 つで、10 になる
		// Child: [1:ID, 2:ChildID, 3:Arr1, 4:CommitedAt, 5:With_Index1] + normalColumnが 7 - 2 つで、10 になる
		{"empty : 7-1000", 7, empty, 1000, false},
		{"empty : 7-1001", 7, empty, 1001, true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			mu, err := createInterleaveWithIndexInsertMutations(tt.normalColumnCount, tt.addColumn, tt.rowCount)
			if err != nil {
				t.Fatal(err)
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

// createInterleaveWithIndexInsertMutations is Interleave Table の Insert Mutationを作成する
// 親と子を1:1で作るので、rowCount * 2 の数が返ってくる
// normalColumnCount を指定することで、INDEXが付いていないカラムの数を調整する
// Measure TableはWithIndex1が1つ, WithIndex2が2つの合計3つのセカンダリインデックスを持ち、INSERT時はセカンダリインデックスを持つカラムがNULLの場合も、セカンダリインデックスがmutationに含まれるので、mutation 数が +3 される
func createInterleaveWithIndexInsertMutations(normalColumnCount int, addColumn map[string]interface{}, rowCount int) ([]*spanner.Mutation, error) {
	var list []*spanner.Mutation
	for i := 0; i < rowCount; i++ {
		parentID, parentMu, err := createInterleaveParentInsertMutation(normalColumnCount, addColumn)
		if err != nil {
			return nil, err
		}
		childMu, err := createInterleaveChildInsertMutation(parentID, normalColumnCount, addColumn)
		if err != nil {
			return nil, err
		}
		list = append(list, parentMu)
		list = append(list, childMu)
	}

	return list, nil
}

func createInterleaveWithIndexParentInsertMutation(normalColumnCount int, addColumn map[string]interface{}) (string, *spanner.Mutation, error) {
	v := make(map[string]interface{})
	id := uuid.New().String()
	v["ID"] = id
	for j := 1; j <= normalColumnCount; j++ {
		v[fmt.Sprintf("Col%d", j)] = ""
	}

	for addKey, addValue := range addColumn {
		v[addKey] = addValue
	}
	v["Arr1"] = []string{}
	v["CommitedAt"] = spanner.CommitTimestamp
	return id, spanner.InsertMap(InterleaveParentWithIndexTable, v), nil
}

func createInterleaveWithIndexChildInsertMutation(parentID string, normalColumnCount int, addColumn map[string]interface{}) (*spanner.Mutation, error) {
	ncc := normalColumnCount - 2 // Parent.ID, With_Index1が増えてるので、2つ減らす
	if ncc < 0 {
		return nil, fmt.Errorf("invalid argument. plz normalColumnCount > 0")
	}

	v := make(map[string]interface{})
	id := uuid.New().String()
	v["ID"] = parentID
	v["ChildID"] = id
	for j := 1; j <= ncc; j++ {
		v[fmt.Sprintf("Col%d", j)] = ""
	}

	for addKey, addValue := range addColumn {
		v[addKey] = addValue
	}
	v["Arr1"] = []string{}
	v["CommitedAt"] = spanner.CommitTimestamp
	return spanner.InsertMap(InterleaveChildWithIndexTable, v), nil
}

func TestMeasureInterleaveWithIndex_Delete(t *testing.T) {
	ctx := context.Background()
	sc := createClient(ctx, t)

	empty := make(map[string]interface{})

	cases := []struct {
		name              string
		normalColumnCount int
		updateColumn      map[string]interface{}
		rowCount          int64
		wantErr           bool
	}{
		// WithIndexをすべてNULLにした時、[1:MeasureParent Table ,3:MeasureChildWithIndex1_1 INDEX Table]で、 2 になる
		{"empty : 7-10000", 7, empty, 10000, false},
		{"empty : 7-10001", 7, empty, 10001, true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var ids []string
			{
				// DELETEするために先にINSERTする
				var mus []*spanner.Mutation
				for i := int64(0); i < tt.rowCount; i++ {
					parentID, parentMu, err := createInterleaveWithIndexParentInsertMutation(tt.normalColumnCount, tt.updateColumn)
					if err != nil {
						t.Fatal(err)
					}
					childMu, err := createInterleaveWithIndexChildInsertMutation(parentID, tt.normalColumnCount, tt.updateColumn)
					if err != nil {
						t.Fatal(err)
					}
					ids = append(ids, parentID)
					mus = append(mus, parentMu)
					mus = append(mus, childMu)
					if len(mus) > 100 {
						_, err = sc.Apply(ctx, mus)
						if err != nil {
							t.Fatal("failed Insert...", err)
						}
						mus = []*spanner.Mutation{}
					}
				}
				if len(mus) > 0 {
					_, err := sc.Apply(ctx, mus)
					if err != nil {
						t.Fatal("failed Insert...", err)
					}
				}
			}
			mu := createDeleteMutation(t, InterleaveParentWithIndexTable, ids, tt.rowCount)
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
