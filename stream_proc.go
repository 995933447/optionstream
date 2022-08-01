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

type onOptValNoneProcFunc func() error
type onOptValBoolProcFunc func(val bool) error
type onOptValInt32ProcFunc func(val int32) error
type onOptValInt32ListProcFunc func(val []int32) error
type onOptValInt64ProcFunc func(val int64) error
type onOptValInt64ListProcFunc func(val []int64) error
type onOptValUint32ProcFunc func(val uint32) error
type onOptValUint32ListProcFunc func(valList []uint32) error
type onOptValUint64ProcFunc func(val uint64) error
type onOptValUint64ListProcFunc func(val []uint64) error
type onOptValStringListProcFunc func(val []string) error
type onOptValStringProcFunc func(val string) error
type onOptValTimestampRangeProcFunc func(beginAt, endAt int64) error
type onOptValAnyProcFunc func(val interface{}) error

type optionProcessor struct {
	valType OptValType
	onProcFunc interface{}
}

func (p *optionProcessor) Process(opt *Option) error {
	procFunc := p.onProcFunc
	switch p.valType {
	case OptValTypeNone:
		return procFunc.(onOptValNoneProcFunc)()
	case OptValTypeBool:
		return procFunc.(onOptValBoolProcFunc)(opt.Val.(bool))
	case OptValTypeInt32:
		return procFunc.(onOptValInt32ProcFunc)(opt.Val.(int32))
	case OptValTypeUint32:
		return procFunc.(onOptValUint32ProcFunc)(opt.Val.(uint32))
	case OptValTypeInt64:
		return procFunc.(onOptValInt64ProcFunc)(opt.Val.(int64))
	case OptValTypeUint64:
		return procFunc.(onOptValUint64ProcFunc)(opt.Val.(uint64))
	case OptValTypeString:
		return procFunc.(onOptValStringProcFunc)(opt.Val.(string))
	case OptValTypeStringList:
		return procFunc.(onOptValStringListProcFunc)(opt.Val.([]string))
	case OptValTypeUint32List:
		return procFunc.(onOptValUint32ListProcFunc)(opt.Val.([]uint32))
	case OptValTypeUint64List:
		return procFunc.(onOptValUint64ListProcFunc)(opt.Val.([]uint64))
	case OptValTypeInt32List:
		return procFunc.(onOptValInt32ListProcFunc)(opt.Val.([]int32))
	case OptValTypeInt64List:
		return procFunc.(onOptValInt64ListProcFunc)(opt.Val.([]int64))
	case OptValTypeTimestampRange:
		timestamps := opt.Val.([]int64)
		var beginAt, endAt int64
		timestampNum := len(timestamps)
		if timestampNum > 0 {
			beginAt = timestamps[0]
		}
		if timestampNum > 1 {
			endAt = timestamps[1]
		}
		return procFunc.(onOptValTimestampRangeProcFunc)(beginAt, endAt)
	case OptValTypeAny:
		return procFunc.(onOptValAnyProcFunc)(opt.Val.([]int64))
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

func (p *StreamProcessor) OnNone(key interface{}, procFunc onOptValNoneProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeNone,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnInt32(key interface{}, procFunc onOptValInt32ProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeInt32,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnInt32List(key interface{}, procFunc onOptValInt32ListProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeInt32List,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnInt64List(key interface{}, procFunc onOptValInt64ListProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeInt64List,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnUint32(key interface{}, procFunc onOptValUint32ProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeUint32,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnInt64(key interface{}, procFunc onOptValInt64ProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeInt64,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnUint64(key interface{}, procFunc onOptValUint64ProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeUint64,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnString(key interface{}, procFunc onOptValStringProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeString,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnStringList(key interface{}, procFunc onOptValStringListProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeStringList,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnUint32List(key interface{}, procFunc onOptValUint32ListProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeUint32List,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnUint64List(key interface{}, procFunc onOptValUint64ProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeUint64List,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnTimestampRange(key interface{}, procFunc onOptValTimestampRangeProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeTimestampRange,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnBool(key interface{}, procFunc onOptValBoolProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeBool,
		onProcFunc: procFunc,
	}
	return p
}

func (p *StreamProcessor) OnAny(key interface{}, procFunc onOptValUint64ProcFunc) *StreamProcessor {
	p.optProcessorMap[key] = &optionProcessor{
		valType: OptValTypeAny,
		onProcFunc: procFunc,
	}
	return p
}

func (p StreamProcessor) Process() error {
	for _, opt := range p.stream.Options {
		optProcessor, ok := p.optProcessorMap[opt.Key]
		if !ok {
			continue
		}

		if err := optProcessor.Process(opt); err != nil {
			return err
		}
	}

	return nil
}
