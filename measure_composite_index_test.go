package mutation_count_playground_test

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
)

const CompositeIndexTable = "MeasureCompositeIndex"

func TestMeasureCompositeIndex_Insert(t *testing.T) {
	ctx := context.Background()
	sc := createClient(ctx, t)

	empty := make(map[string]interface{})
	withCompositeIndex := map[string]interface{}{"WithCompositeIndex1": ""}

	cases := []struct {
		name              string
		normalColumnCount int
		addColumn         map[string]interface{}
		rowCount          int
		wantErr           bool
	}{
		// WithIndexをすべてNULLにした時、 [1:ID, 2:Arr1, 3:CommitedAt, 4:MeasureWithIndex1_1, 5:MeasureWithIndex2_1, 6:MeasureWithIndex2_2, 7:MeasureCompositeIndexWithCompositeIndex] + normalColumnが 3 つで、10 になる
		{"empty : 3-2000", 3, empty, 2000, false},
		{"empty : 3-2001", 3, empty, 2001, true},

		// MeasureCompositeIndexWithCompositeIndexに値を入れた時、 [1:ID, 2:Arr1, 3:CommitedAt, 4:WithCompositeIndex1, 5:MeasureWithIndex1_1, 6:MeasureWithIndex2_1, 7:MeasureWithIndex2_2, 8:MeasureCompositeIndexWithCompositeIndex] + normalColumnが 2 つで、10 になる
		{"WithCompositeIndex1 : 2-2000", 2, withCompositeIndex, 2000, false},
		{"WithCompositeIndex1 : 2-2001", 2, withCompositeIndex, 2001, true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			mu, err := createInsertMutation(CompositeIndexTable, tt.normalColumnCount, tt.addColumn, tt.rowCount)
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

func TestMeasureCompositeIndex_Update(t *testing.T) {
	ctx := context.Background()
	sc := createClient(ctx, t)

	empty := make(map[string]interface{})
	withCompositeIndex := map[string]interface{}{"WithCompositeIndex1": "", "WithCompositeIndex2": ""}

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

		// WithIndex1に値を入れて、WithIndex2をNULLにした時、 [1:ID, 2:Arr1, 3:CommitedAt, 4:WithCompositeIndex1, , 5:WithCompositeIndex2, 6:MeasureCompositeIndexWithCompositeIndex, 7:?] + normalColumnが 3 つで、10 になる
		{"withIndex1 : 3-2000", 3, withCompositeIndex, 2000, false},
		{"withIndex1 : 3-2001", 3, withCompositeIndex, 2001, true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var ids []string
			{
				// UPDATEするために先にINSERTする
				var mu []*spanner.Mutation
				var err error
				ids, mu, err = createInsertMutationForUpdateTest(CompositeIndexTable, tt.rowCount)
				if err != nil {
					t.Fatal(err)
				}
				_, err = sc.Apply(ctx, mu)
			}
			mu := createUpdateMutation(t, CompositeIndexTable, ids, tt.normalColumnCount, tt.updateColumn, tt.rowCount)
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
