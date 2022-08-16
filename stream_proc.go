package optionstream

type OptValType int

const (
	OptValTypeNone OptValType = iota
	OptValTypeUint32
	OptValTypeString
	OptValTypeUint32List
	OptValTypeUint64
	OptValTypeInt64
	OptValTypeInt64List
	OptValTypeTimestampRange
	OptValTypeUint64List
	OptValTypeBool
	OptValTypeStringList
	OptValTypeInt32
	OptValTypeInt32List
	OptValTypeAny
)

type OnOptValNoneProcFunc func() error
type OnOptValBoolProcFunc func(val bool) error
type OnOptValInt32ProcFunc func(val int32) error
type OnOptValInt32ListProcFunc func(val []int32) error
type OnOptValInt64ProcFunc func(val int64) error
type OnOptValInt64ListProcFunc func(val []int64) error
type OnOptValUint32ProcFunc func(val uint32) error
type OnOptValUint32ListProcFunc func(valList []uint32) error
type OnOptValUint64ProcFunc func(val uint64) error
type OnOptValUint64ListProcFunc func(val []uint64) error
type OnOptValStringListProcFunc func(val []string) error
type OnOptValStringProcFunc func(val string) error
type OnOptValTimestampRangeProcFunc func(beginAt, endAt int64) error
type OnOptValAnyProcFunc func(val interface{}) error

type optionProcessor struct {
	valType OptValType
	onProcFunc interface{}
}

func (p *optionProcessor) Process(option *Option) error {
	procFunc := p.onProcFunc
	switch p.valType {
	case OptValTypeNone:
		return procFunc.(OnOptValNoneProcFunc)()
	case OptValTypeBool:
		return procFunc.(OnOptValBoolProcFunc)(option.Val.(bool))
	case OptValTypeInt32:
		return procFunc.(OnOptValInt32ProcFunc)(option.Val.(int32))
	case OptValTypeUint32:
		return procFunc.(OnOptValUint32ProcFunc)(option.Val.(uint32))
	case OptValTypeInt64:
		return procFunc.(OnOptValInt64ProcFunc)(option.Val.(int64))
	case OptValTypeUint64:
		return procFunc.(OnOptValUint64ProcFunc)(option.Val.(uint64))
	case OptValTypeString:
		return procFunc.(OnOptValStringProcFunc)(option.Val.(string))
	case OptValTypeStringList:
		return procFunc.(OnOptValStringListProcFunc)(option.Val.([]string))
	case OptValTypeUint32List:
		return procFunc.(OnOptValUint32ListProcFunc)(option.Val.([]uint32))
	case OptValTypeUint64List:
		return procFunc.(OnOptValUint64ListProcFunc)(option.Val.([]uint64))
	case OptValTypeInt32List:
		return procFunc.(OnOptValInt32ListProcFunc)(option.Val.([]int32))
	case OptValTypeInt64List:
		return procFunc.(OnOptValInt64ListProcFunc)(option.Val.([]int64))
	case OptValTypeTimestampRange:
		timestamps := option.Val.([]int64)
		var beginAt, endAt int64
		timestampNum := len(timestamps)
		if timestampNum > 0 {
			beginAt = timestamps[0]
		}
		if timestampNum > 1 {
			endAt = timestamps[1]
		}
		return procFunc.(OnOptValTimestampRangeProcFunc)(beginAt, endAt)
	case OptValTypeAny:
		return procFunc.(OnOptValAnyProcFunc)(option.Val.([]int64))
	default:
		panic("not support value type")
	}
}

type StreamProcessor struct {
	stream *Stream
	optProcessorMap map[interface{}]*optionProcessor
}

func NewStreamProcessor(stream *Stream) *StreamProcessor {
	return &StreamProcessor{
		stream: stream,
		optProcessorMap: make(map[interface{}]*optionProcessor),
	}
}

func (p *StreamProcessor) OnNone(key interface{}, procFunc OnOptValNoneProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeNone,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnInt32(key interface{}, procFunc OnOptValInt32ProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeInt32,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnInt32List(key interface{}, procFunc OnOptValInt32ListProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeInt32List,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnInt64List(key interface{}, procFunc OnOptValInt64ListProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeInt64List,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnUint32(key interface{}, procFunc OnOptValUint32ProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeUint32,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnInt64(key interface{}, procFunc OnOptValInt64ProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeInt64,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnUint64(key interface{}, procFunc OnOptValUint64ProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeUint64,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnString(key interface{}, procFunc OnOptValStringProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeString,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnStringList(key interface{}, procFunc OnOptValStringListProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeStringList,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnUint32List(key interface{}, procFunc OnOptValUint32ListProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeUint32List,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnUint64List(key interface{}, procFunc OnOptValUint64ListProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeUint64List,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnTimestampRange(key interface{}, procFunc OnOptValTimestampRangeProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeTimestampRange,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnBool(key interface{}, procFunc OnOptValBoolProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeBool,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnAny(key interface{}, procFunc OnOptValUint64ProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeAny,
		onProcFunc: procFunc,
	}
	return p
}

func (p StreamProcessor) Process() error {
	for _, option := range p.stream.Options {
		optProcessor, ok := p.optProcessorMap[option.Key]
		if !ok {
			continue
		}

		if err := optProcessor.Process(option); err != nil {
			return err
		}
	}

	return nil
}
