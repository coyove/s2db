package fts

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
	"unsafe"

	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/sego"
	"github.com/golang/protobuf/proto"
)

var (
	seg    sego.Segmenter
	st     sego.StopTokens
	loader sync.Once
)

func LoadDict(words []string) {
	var extraWords, extraStopWords []byte
	for _, w := range words {
		if strings.HasPrefix(w, "$.") {
			extraStopWords = append(extraStopWords, w...)
			extraStopWords = append(extraStopWords, '\n')
		} else {
			extraWords = append(extraWords, w...)
			extraWords = append(extraWords, []byte(" 10 n\n")...)
		}
	}
	var seg2 sego.Segmenter
	var st2 sego.StopTokens
	seg2.LoadDictionary(extraWords)
	st2.Init(extraStopWords)
	seg = seg2
	st = st2
}

type Document struct {
	NumTokens int64        `protobuf:"varint,1,opt,name=num_tokens"`
	Tokens    []*Segmented `protobuf:"bytes,2,rep,name=tokens"`
	Prefixs   []string     `protobuf:"bytes,3,rep,name=prefixs"`
}

type Segmented struct {
	Count int64  `protobuf:"varint,1,opt,name=count"`
	Token string `protobuf:"bytes,2,opt,name=token"`
}

func (doc *Document) Reset()                  { *doc = Document{} }
func (doc *Document) String() string          { return proto.CompactTextString(doc) }
func (*Document) ProtoMessage()               {}
func (*Document) Descriptor() ([]byte, []int) { return nil, []int{0} }

func (doc *Document) MarshalBinary() []byte {
	buf, err := proto.Marshal(doc)
	s2pkg.PanicErr(err)
	return buf
}

var segMap = sync.Pool{
	New: func() interface{} {
		return map[string]int{}
	},
}

func SplitSimple(content string) (res []string) {
	for _, t := range Split(content).Tokens {
		res = append(res, t.Token)
	}
	return
}

func Split(content string) (doc Document) {
	if content == "" {
		return
	}

	loader.Do(func() {
		seg.LoadDictionary(nil)
		st.Init(nil)
	})

	buf := struct {
		s string
		a int
	}{content, len(content)}

	tmp := segMap.Get().(map[string]int)
	for _, s := range seg.Segment(*(*[]byte)(unsafe.Pointer(&buf))) {
		t := s.Token().Text()
		if t == "" || st.IsStopToken(t) || notPlane0(t) || !utf8.ValidString(t) {
			continue
		}
		if v, err := strconv.Atoi(t); err == nil && v < 100 {
			continue
		}
		tmp[t]++
		doc.NumTokens++
	}
	for k, sz := range tmp {
		delete(tmp, k)
		doc.Tokens = append(doc.Tokens, &Segmented{Token: k, Count: int64(sz)})
	}
	segMap.Put(tmp)

	sort.Slice(doc.Tokens, func(i, j int) bool {
		return doc.Tokens[i].Token <= doc.Tokens[j].Token
	})
	return
}

func notPlane0(t string) bool {
	for len(t) > 0 {
		r, sz := utf8.DecodeRuneInString(t)
		if r > 0xffff {
			return true
		}
		t = t[sz:]
	}
	return false
}

func (d *Document) Valid() bool {
	return len(d.Tokens) > 0 && d.NumTokens > 0
}

func (d *Document) SizeBytes() (total int) {
	sz := int(unsafe.Sizeof(0))
	for _, s := range d.Tokens {
		total += len(s.Token) + sz*3
	}
	total += sz
	return
}

func (d Document) TermFreq(st string) float64 {
	idx := sort.Search(len(d.Tokens), func(i int) bool {
		return d.Tokens[i].Token >= st
	})
	if idx < len(d.Tokens) && d.Tokens[idx].Token == st {
		return float64(d.Tokens[idx].Count+1) / float64(d.NumTokens+1)
	}
	return 0
}
