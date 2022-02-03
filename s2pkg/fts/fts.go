package fts

import (
	"sort"
	"strconv"
	"sync"
	"unicode/utf8"
	"unsafe"

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
	NumTokens int
	Tokens    []Segmented
}

type Segmented struct {
	Token bas.Value
	Count int
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
