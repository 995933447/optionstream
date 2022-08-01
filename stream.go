package optionstream

type Option struct {
	Key interface{}
	Val interface{}
}

type Stream struct {
	Options []*Option
	optionMap map[interface{}]*Option
}

func NewStream() *Stream {
	return &Stream {
		optionMap: make(map[interface{}]*Option),
	}
}

func (s *Stream) SetOption(key interface{}, val interface{}) *Stream {
	opt, ok := s.optionMap[key]
	if ok {
		opt.Val = val
	} else {
		opt = &Option{key, val}
		s.Options = append(s.Options, opt)
	}
	s.optionMap[key] = opt
	return s
}

func (s *Stream) Option(key interface{}) (*Option, bool) {
	option, ok := s.optionMap[key]
	return option, ok
}

func (s *Stream) CopyStream(otherStream *Stream, optKey interface{}, otherStreamOptKey interface{}) *Stream {
	otherStreamOpt, ok := otherStream.Option(otherStreamOptKey)
	if ok {
		s.SetOption(optKey, otherStreamOpt.Val)
	}
	return s
}

type QueryStream struct {
	*Stream
	Limit int64
	Offset int64
}

func NewQueryStream() *QueryStream {
	return &QueryStream{
		Stream: NewStream(),
	}
}

func (s *QueryStream) SetLimit(limit int64) *QueryStream {
	s.Limit = limit
	return s
}

func (s *QueryStream) SetOffset(offset int64) *QueryStream {
	s.Offset = offset
	return s
}