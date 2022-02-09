package fts

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"unicode/utf8"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/coyove/s2db/sego"
	"github.com/golang/protobuf/proto"
)

var (
	extraWords     []byte
	extraStopWords []byte
	seg            sego.Segmenter
	st             sego.StopTokens
	loaded         sync.WaitGroup
)

func init() {
	loaded.Add(1)
	go func() {
		seg.LoadDictionary(nil)
		st.Init(nil)
		loaded.Done()
	}()
}

func AddWords(ws ...string) {
	for _, w := range ws {
		extraWords = append(extraWords, []byte(w+" 10 n\n")...)
	}
	loaded.Wait()
	var seg2 sego.Segmenter
	seg2.LoadDictionary(extraWords)
	seg = seg2
}

func AddStopWords(ws ...string) {
	for _, w := range ws {
		extraStopWords = append(extraStopWords, []byte(w+"\n")...)
	}
	loaded.Wait()
	var st2 sego.StopTokens
	st2.Init(extraStopWords)
	st = st2
}

type Document struct {
	NumTokens int64        `protobuf:"varint,1,opt,name=num_tokens"`
	Tokens    []*Segmented `protobuf:"bytes,2,rep,name=tokens"`
}

type Segmented struct {
	Count int64  `protobuf:"varint,1,opt,name=count"`
	Token string `protobuf:"bytes,2,opt,name=token"`
}

func (doc *Document) Reset()                  { *doc = Document{} }
func (doc *Document) String() string          { return proto.CompactTextString(doc) }
func (*Document) ProtoMessage()               {}
func (*Document) Descriptor() ([]byte, []int) { return nil, []int{0} }

type RevertedIndex struct {
	Bitmap      *roaring.Bitmap
	Cardinality int
}

func (ri *RevertedIndex) Marshal() []byte {
	p := bytes.Buffer{}
	binary.Write(&p, binary.BigEndian, uint32(ri.Cardinality))
	buf, _ := ri.Bitmap.MarshalBinary()
	p.Write(buf)
	return p.Bytes()
}

func (ri *RevertedIndex) Unmarshal(p []byte) error {
	if len(p) < 4 {
		return fmt.Errorf("RevertedIndex: invalid buffer")
	}
	ri.Cardinality = int(binary.BigEndian.Uint32(p))
	ri.Bitmap = roaring.New()
	return ri.Bitmap.UnmarshalBinary(p[4:])
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

	loaded.Wait()
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
