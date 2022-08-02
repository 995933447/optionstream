package optionstream

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func TestStream(t *testing.T) {
	stream := NewStream(nil).SetOption("test_key", "abc")

	err := NewStreamProcessor(stream).
		OnString("test_key", func(val string) error {
			fmt.Println(val)
			return nil
		}).
		Process()
	if err != nil {
		t.Fatal(err)
	}
}

type queryHandler struct {

}

func(*queryHandler) Hit(ctx context.Context, limit, offset int64, list interface{}) (int64, error) {
	listInt64 := list.(*[]int64)
	*listInt64 = []int64{limit, offset}
	return 10, nil
}

func(q *queryHandler) Query(ctx context.Context, limit, offset int64, list interface{}) error {
	_, _ = q.Hit(ctx, limit, offset, list)
	return nil
}

func TestQueryStream(t *testing.T) {
	queryStream := NewQueryStream(nil).
		SetLimit(15).
		SetOffset(5).
		SetOption("test_key", "abc").
		SetOption(123, 123)
	j, err := json.Marshal(queryStream)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("j:%s", string(j))

	queryStreamProcessor := NewQueryStreamProcessor(queryStream)
	queryStreamProcessor.
		OnString("test_key", func(val string) error {
			fmt.Println(val)
			return nil
		}).OnInt32(int32(123), func(val int32) error {
			fmt.Println(val)
			return nil
		})

	var (
		pagination *Pagination
		list []int64
	)
	pagination, err = queryStreamProcessor.PaginateFrom(context.TODO(), &queryHandler{}, &list)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("paginate:%+v list:%+v", pagination, list)

	var list2 []int64
	err = queryStreamProcessor.QueryFrom(context.TODO(), &queryHandler{}, &list2)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("list:%+v", list2)
}
