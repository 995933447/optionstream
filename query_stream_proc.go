package optionstream

import (
	"context"
)

type Pagination struct {
	offset int64 `access:r`
	limit int64	`access:r`
	total int64 `access:r`
}

func (p Pagination) Offset() int64 {
	return p.offset
}

func (p Pagination) Limit() int64 {
	return p.limit
}

func (p Pagination) Total() int64 {
	return p.total
}

type Queriable interface {
	Hit(ctx context.Context, limit, offset int64, list interface{}) (int64, error)
	Query(ctx context.Context, limit, offset int64, list interface{}) error
}

type QueryStreamProcessor struct {
	*StreamProcessor
	stream *QueryStream
}

func NewQueryStreamProcessor(stream *QueryStream) *QueryStreamProcessor {
	return &QueryStreamProcessor{
		stream: stream,
		StreamProcessor: NewStreamProcessor(stream.Stream),
	}
}

func (p QueryStreamProcessor) PaginateFrom(ctx context.Context, queriable Queriable, list interface{}) (*Pagination, error) {
	var err error
	if err = p.Process(); err != nil {
		return nil, err
	}

	var hitTotal int64
	if hitTotal, err = queriable.Hit(ctx, p.stream.Limit, p.stream.Offset, list); err != nil {
		return nil, err
	}

	var paginate Pagination
	paginate.total = hitTotal
	paginate.limit = p.stream.Limit
	paginate.offset = p.stream.Offset
	return &paginate, nil
}

func (p QueryStreamProcessor) QueryFrom(ctx context.Context, queriable Queriable, list interface{}) error {
	if err := p.Process(); err != nil {
		return err
	}

	if err := queriable.Query(ctx, p.stream.Limit, p.stream.Offset, list); err != nil {
		return err
	}

	return nil
}