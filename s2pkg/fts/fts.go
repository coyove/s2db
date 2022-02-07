package fts

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"unicode/utf8"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/sego"
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
	NumTokens int64
	Tokens    []Segmented
}

type Segmented struct {
	Count int
	Token bas.Value
}

func (doc Document) Marshal() []byte {
	p := &bytes.Buffer{}
	binary.Write(p, binary.BigEndian, doc.NumTokens)
	binary.Write(p, binary.BigEndian, uint32(len(doc.Tokens)))
	for _, seg := range doc.Tokens {
		binary.Write(p, binary.BigEndian, uint32(seg.Count))
		binary.Write(p, binary.BigEndian, uint32(seg.Token.StrLen()))
		p.WriteString(seg.Token.Str())
	}
	return p.Bytes()
}

func (doc *Document) Unmarshal(p []byte) error {
	rd := bytes.NewReader(p)
	if err := binary.Read(rd, binary.BigEndian, &doc.NumTokens); err != nil {
		return err
	}
	var tokensLen uint32
	if err := binary.Read(rd, binary.BigEndian, &tokensLen); err != nil {
		return err
	}
	doc.Tokens = make([]Segmented, tokensLen)
	for i := 0; i < int(tokensLen); i++ {
		var seg Segmented
		var count uint32
		if err := binary.Read(rd, binary.BigEndian, &count); err != nil {
			return err
		}
		seg.Count = int(count)
		if err := binary.Read(rd, binary.BigEndian, &count); err != nil {
			return err
		}
		buf := make([]byte, count)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return err
		}
		seg.Token = bas.UnsafeStr(buf)
		doc.Tokens[i] = seg
	}
	return nil
}

type RevertedIndex struct {
	Bitmap      *roaring.Bitmap
	Cardinality int
}

func (ri RevertedIndex) Marshal() []byte {
	p := &bytes.Buffer{}
	binary.Write(p, binary.BigEndian, uint32(ri.Cardinality))
	ri.Bitmap.WriteTo(p)
	return p.Bytes()
}

func (ri *RevertedIndex) Unmarshal(p []byte) error {
	if len(p) < 4 {
		return fmt.Errorf("RevertedIndex: invalid bytes")
	}
	ri.Cardinality = int(binary.BigEndian.Uint32(p[:4]))
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
		res = append(res, t.Token.Str())
	}
	return
}

func Split(content string) (doc Document) {
	loaded.Wait()
	buf := struct {
		s string
		a int
	}{content, len(content)}

	tmp := segMap.Get().(map[string]int)
	for _, s := range seg.Segment(*(*[]byte)(unsafe.Pointer(&buf))) {
		t := s.Token().Text()
		if t == "" || st.IsStopToken(t) || notPlane0(t) {
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
		doc.Tokens = append(doc.Tokens, Segmented{Token: bas.Str(k), Count: sz})
	}
	segMap.Put(tmp)

	sort.Slice(doc.Tokens, func(i, j int) bool {
		return bas.Less(doc.Tokens[i].Token, doc.Tokens[j].Token)
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
		x := s.Token.StrLen()
		if x <= 8 {
			x = 0
		} else {
			x += 16
		}
		total += x + sz*3
	}
	total += sz
	return
}

func (d Document) TermFreq(st bas.Value) float64 {
	idx := sort.Search(len(d.Tokens), func(i int) bool {
		return bas.Less(st, d.Tokens[i].Token) || st.Equal(d.Tokens[i].Token)
	})
	if idx < len(d.Tokens) && d.Tokens[idx].Token.Equal(st) {
		return float64(d.Tokens[idx].Count+1) / float64(d.NumTokens+1)
	}
	return 0
}
