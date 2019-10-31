package mutation_count_playground_test

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
)

const StoringIndexTable = "MeasureWithStoring"

func TestMeasureStoringIndex_Insert(t *testing.T) {
	ctx := context.Background()
	sc := createClient(ctx, t)

	empty := make(map[string]interface{})
	withStoringIndex1 := map[string]interface{}{"WithIndex1": ""}
	withStoringIndex2 := map[string]interface{}{"WithIndex2": ""}
	withStoringIndex1and2 := map[string]interface{}{"WithIndex1": "", "WithIndex2": ""}

	cases := []struct {
		name              string
		normalColumnCount int
		addColumn         map[string]interface{}
		rowCount          int
		wantErr           bool
	}{
		// WithIndexをすべてNULLにした時、 [1:ID, 2:Arr1, 3:CommitedAt, 4:MeasureWithStoringWithIndex1_1, 5:MeasureWithStoringWithIndex2_1] + normalColumnが 5 つで、10 になる
		{"empty : 5-2000", 5, empty, 2000, false},
		{"empty : 5-2001", 5, empty, 2001, true},

		//WithIndexに値を入れた時、 [1:ID, 2:Arr1, 3:CommitedAt, 4:WithIndex1, 5:MeasureWithStoringWithIndex1_1, 6:MeasureWithStoringWithIndex2_1] + normalColumnが 4 つで、10 になる
		{"WithIndex1 : 4-2000", 4, withStoringIndex1, 2000, false},
		{"WithIndex1 : 4-2001", 4, withStoringIndex1, 2001, true},

		//WithIndexに値を入れた時、 [1:ID, 2:Arr1, 3:CommitedAt, 4:WithIndex2, 5:MeasureWithStoringWithIndex1_1, 6:MeasureWithStoringWithIndex2_1] + normalColumnが 4 つで、10 になる
		{"WithIndex2 : 3-2000", 4, withStoringIndex2, 2000, false},
		{"WithIndex2 : 3-2001", 4, withStoringIndex2, 2001, true},

		//WithIndexに値を入れた時、 [1:ID, 2:Arr1, 3:CommitedAt, 4:WithIndex1, 5:WithIndex2, 6:MeasureWithStoringWithIndex1_1, 7:MeasureWithStoringWithIndex2_1] + normalColumnが 3 つで、10 になる
		{"withIndex1and2 : 3-2000", 3, withStoringIndex1and2, 2000, false},
		{"withIndex1and2 : 3-2001", 3, withStoringIndex1and2, 2001, true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			mu, err := createInsertMutation(StoringIndexTable, tt.normalColumnCount, tt.addColumn, tt.rowCount)
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

func TestMeasureStoringIndex_Update(t *testing.T) {
	ctx := context.Background()
	sc := createClient(ctx, t)

	empty := make(map[string]interface{})
	withStoringIndex := map[string]interface{}{"WithIndex1": ""}

	cases := []struct {
		name              string
		normalColumnCount int
		updateColumn      map[string]interface{}
		rowCount          int64
		wantErr           bool
	}{
		// WithIndexをすべてNULLにした時、 [1:ID, 2:Arr1, 3:CommitedAt] + normalColumnが 7 つで、10 になる
		{"empty : 7-2000", 7, empty, 2000, false},
		{"empty : 7-2001", 7, empty, 2001, true},

		// WithIndex1に値を入れて、WithIndex2をNULLにした時、 [1:ID, 2:Arr1, 3:CommitedAt, 4:WithStoringIndex1, , 5:WithStoringIndex2, 6:MeasureStoringIndexWithStoringIndex * 2] + normalColumnが 3 つで、10 になる
		{"withIndex1 : 3-2000", 3, withStoringIndex, 2000, false},
		{"withIndex1 : 3-2001", 3, withStoringIndex, 2001, true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var ids []string
			{
				// UPDATEするために先にINSERTする
				var mu []*spanner.Mutation
				var err error
				ids, mu, err = createInsertMutationForUpdateTest(StoringIndexTable, tt.rowCount)
				if err != nil {
					t.Fatal(err)
				}
				_, err = sc.Apply(ctx, mu)
			}
			mu := createUpdateMutation(t, StoringIndexTable, ids, tt.normalColumnCount, tt.updateColumn, tt.rowCount)
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
