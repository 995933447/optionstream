package optionstream

type Option struct {
	Key interface{}
	Val interface{}
}

type Stream struct {
	Options []*Option `json:"options"`
	optionMap map[interface{}]*Option
}

func NewStream(options []*Option) *Stream {
	s := &Stream {
		optionMap: make(map[interface{}]*Option),
		Options: options,
	}
	for _, option := range options {
		s.optionMap[option.Key] = option
	}
	return s
}

func (s *Stream) SetOption(key interface{}, val interface{}) *Stream {
	option, ok := s.optionMap[key]
	if ok {
		option.Val = val
	} else {
		option = &Option{key, val}
		s.Options = append(s.Options, option)
	}
	s.optionMap[key] = option
	return s
}

func (s *Stream) Option(key interface{}) (*Option, bool) {
	option, ok := s.optionMap[key]
	return option, ok
}

func (s *Stream) CopyStream(otherStream *Stream, optionKey interface{}, otherStreamOptionKey interface{}) *Stream {
	otherStreamOption, ok := otherStream.Option(otherStreamOptionKey)
	if ok {
		s.SetOption(optionKey, otherStreamOption.Val)
	}
	return s
}

type QueryStream struct {
	*Stream
	Limit int64 `json:"limit"`
	Offset int64 `json:"offset"`
}

func NewQueryStream(options []*Option) *QueryStream {
	return &QueryStream{
		Stream: NewStream(options),
	}
}

func (s *QueryStream) SetOption(key interface{}, val interface{}) *QueryStream {
	s.Stream.SetOption(key, val)
	return s
}

func (s *QueryStream) CopyStream(otherStream *Stream, optionKey interface{}, otherStreamOptionKey interface{}) *QueryStream {
	s.Stream.CopyStream(otherStream, optionKey, otherStreamOptionKey)
	return s
}

func (s *QueryStream) SetLimit(limit int64) *QueryStream {
	s.Limit = limit
	return s
}

func (s *QueryStream) SetOffset(offset int64) *QueryStream {
	s.Offset = offset
	return s
}