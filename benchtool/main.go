package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/go-redis/redis/v8"
	"golang.org/x/text/unicode/norm"
)

var (
	addr             = flag.String("addr", ":6379", "benchmark endpoint")
	benchmark        = flag.String("n", "", "")
	benchmarkClients = flag.Int("clients", 100, "")
	benchmarkOp      = flag.Int("op", 10000, "")
	benchmarkDefer   = flag.Bool("defer", false, "")
	corpus           = flag.String("corpus", "", "")
	ftsQuery         = flag.String("query", "", "")
	ftsQueryTimeout  = flag.String("query-timeout", "1s", "")
	multiKeysNum     = flag.Int("keysnum", 10, "")
)

func panicErr(err error) {
	if err != nil {
		panic(err)
		os.Exit(-1)
	}
}

func main() {
	ctx := context.TODO()

	flag.Parse()
	rand.Seed(time.Now().Unix())

	rdb := redis.NewClient(&redis.Options{
		Addr:        *addr,
		DialTimeout: time.Second / 2,
	})

	start := time.Now()
	switch *benchmark {
	case "zset_write_one":
		wg := sync.WaitGroup{}
		for i := 0; i < *benchmarkClients; i++ {
			wg.Add(1)
			go func(i int) {
				fmt.Println("client #", i)
				for c := 0; c < *benchmarkOp; c++ {
					m := &redis.Z{Member: strconv.Itoa(c + i**benchmarkClients), Score: rand.Float64()*10 - 5}
					rdb.ZAdd(ctx, "bench", m)
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	case "zset_write_multi":
		wg := sync.WaitGroup{}
		for i := 0; i < *benchmarkClients; i++ {
			wg.Add(1)
			go func(i int) {
				fmt.Println("client #", i)
				for c := 0; c < *benchmarkOp; c++ {
					key := fmt.Sprintf("bench%d", rand.Intn(*multiKeysNum))
					m := &redis.Z{Member: strconv.Itoa(c + i**benchmarkClients), Score: rand.Float64()*10 - 5}
					if *benchmarkDefer {
						rdb.Do(ctx, "ZADD", key, "--defer--", m.Score, m.Member)
					} else {
						rdb.ZAdd(ctx, key, m)
					}
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	case "set_write_multi_many", "zset_write_multi_many":
		cmd := "ZADD"
		if *benchmark == "set_write_multi_many" {
			cmd = "SADD"
		}
		N := 100
		wg := sync.WaitGroup{}
		for i := 0; i < *benchmarkClients; i++ {
			wg.Add(1)
			go func(i int) {
				fmt.Println("client #", i)
				for c := 0; c < *benchmarkOp/N; c++ {
					key := fmt.Sprintf("bench%d", rand.Intn(*multiKeysNum))
					args := []interface{}{cmd, key, "--defer--"}
					for ib := 0; ib < N; ib++ {
						if cmd == "SADD" {
							args = append(args, strconv.Itoa((c+i**benchmarkClients)*N+ib))
						} else {
							args = append(args, rand.Float64()*10-5, strconv.Itoa((c+i**benchmarkClients)*N+ib))
						}
					}
					rdb.Do(ctx, args...)
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	case "zset_write_pipe":
		wg := sync.WaitGroup{}
		for i := 0; i < *benchmarkClients; i++ {
			wg.Add(1)
			go func(i int) {
				fmt.Println("client #", i)
				p := rdb.Pipeline()
				for c := 0; c < 100; c++ {
					p.ZAdd(ctx, "bench", &redis.Z{Member: strconv.Itoa(c), Score: rand.Float64()*10 - 5})
				}
				_, err := p.Exec(ctx)
				if err != nil {
					fmt.Println(err)
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	case "zset_seq_write_1":
		for i := 0; i < 1000; i += 1 {
			args := []interface{}{"ZADD", "seqbench"}
			for c := 0; c < 100; c++ {
				args = append(args, i*100+c, fmt.Sprintf("s%09d", i*100+c))
			}
			cmd := redis.NewStringCmd(ctx, args...)
			rdb.Process(ctx, cmd)
			if cmd.Err() != nil {
				fmt.Println(i, cmd.Err())
			}
		}
	case "fts_test":
		res, _ := split(*corpus)
		fmt.Println(res)
	case "fts_index":
		ch := make(chan [3]interface{})
		for i := 0; i < 10; i++ {
			go func() {
				p := rdb.Pipeline()
				c := 0
				for kvid := range ch {
					p.Do(ctx, "ZADD", "fts:"+kvid[0].(string), "--defer--", kvid[1].(float64), kvid[2].(int))
					if c++; c == 100 {
						if _, err := p.Exec(ctx); err != nil {
							fmt.Println("error:", err)
						}
						p = rdb.Pipeline()
						c = 0
					}
				}
				if c > 0 {
					if _, err := p.Exec(ctx); err != nil {
						panic(err)
					}
				}
			}()
		}
		f, err := os.Open(*corpus)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		rd := bufio.NewReader(f)
		for ln := 0; ; ln++ {
			line, _ := rd.ReadString('\n')
			if len(line) == 0 {
				break
			}
			if ln < 520919 {
				continue
			}
			res, _ := split(line)
			fmt.Println(ln, "count=", len(res))

			for k, v := range res {
				ch <- [3]interface{}{k, v, ln}
			}
		}
	case "fts_query":
		args := []interface{}{"ZRI", 1000, *ftsQueryTimeout}
		walk(*ftsQuery, func(m byte, q string) {
			ng, _ := split(q)
			for k := range ng {
				args = append(args, string(m)+"fts:"+k)
			}
		})

		res, _ := rdb.Do(ctx, args...).Result()
		fetchDiff := time.Since(start).Seconds()

		h := [][]interface{}{}
		for i, id := range res.([]interface{}) {
			if i%2 == 0 {
				ln, _ := strconv.Atoi(fmt.Sprint(id))
				h = append(h, []interface{}{ln, i / 2, "", res.([]interface{})[i+1]})
			}
		}
		sort.Slice(h, func(i, j int) bool {
			return h[i][0].(int) < h[j][0].(int)
		})

		f, err := os.Open(*corpus)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		rd := bufio.NewReader(f)
		for ln, h2 := 0, h; len(h2) > 0; ln++ {
			line, err := rd.ReadString('\n')
			if err != nil {
				break
			}
			if ln == h2[0][0].(int) {
				h2[0][2] = line
				h2 = h2[1:]
			}
		}

		sort.Slice(h, func(i, j int) bool {
			return h[i][1].(int) < h[j][1].(int)
		})
		for _, row := range h {
			fmt.Println(row[0], row[3], row[2])
		}
		fmt.Println("results fecthed in", fetchDiff, "s")
	}
	fmt.Println("finished in", time.Since(start).Seconds(), "s")
}

func walk(text string, f func(byte, string)) {
	text = strings.TrimSpace(text)
	for len(text) > 0 {
		idx := strings.IndexByte(text, ' ')
		var q string
		if idx == -1 {
			q, text = text, ""
		} else {
			q, text = text[:idx], text[idx+1:]
		}
		if len(q) == 0 {
			continue
		}
		switch q[0] {
		case '-', '+', '?':
			f(q[0], q[1:])
		default:
			f('?', q)
		}
	}
}

func split(text string) (res map[string]float64, err error) {
	res = map[string]float64{}

	tmpbuf := bytes.Buffer{}
	total := 0
	lastSplitText := ""

	splitter := func(v string) {
		if v == "" {
			return
		}

		r, _ := utf8.DecodeRuneInString(v)
		if lastSplitText != "" {
			lastr, _ := utf8.DecodeLastRuneInString(lastSplitText)
			if (lastr <= utf8.RuneSelf) != (r <= utf8.RuneSelf) {
				tmpbuf.Reset()
				tmpbuf.WriteRune(lastr)
				tmpbuf.WriteRune(r)
				res[strings.ToLower(tmpbuf.String())]++
				total++
			}
		}
		// fmt.Println(lastSplitText, v)
		lastSplitText = v

		if r < utf8.RuneSelf {
			if len(v) == 1 {
				return
			}
			res[strings.ToLower(v)]++
			total++
			return
		}

		lastr := utf8.RuneError
		runeCount, old := 0, v
		for len(v) > 0 {
			r, sz := utf8.DecodeRuneInString(v)
			v = v[sz:]

			if lastr != utf8.RuneError {
				tmpbuf.Reset()
				tmpbuf.WriteRune(lastr)
				tmpbuf.WriteRune(r)
				res[tmpbuf.String()]++
				total++
			}

			lastr = r
			runeCount++
		}

		if runeCount <= 4 {
			res[old]++
		}
	}

	lasti, i, lastr := 0, 0, utf8.RuneError
	for i < len(text) {
		r, sz := utf8.DecodeRuneInString(text[i:])
		if r == utf8.RuneError {
			return nil, fmt.Errorf("grouping: invalid UTF-8 string: %q", text)
		}

		// fmt.Println(string(lastr), string(r), isdiff(lastr, r))
		if lastr != utf8.RuneError {
			isdiff := false
			if ac, bc := Continue(lastr), Continue(r); ac != bc {
				isdiff = true
			}
			if ac, bc := lastr <= utf8.RuneSelf, r <= utf8.RuneSelf; ac != bc {
				isdiff = true
			}
			if isdiff {
				splitter(text[lasti:i])
				lasti = i
			}
		}
		i += sz

		if Continue(r) {
			lastr = r
		} else {
			lastr = utf8.RuneError
			lasti = i
		}
	}
	splitter(text[lasti:])

	for k, v := range res {
		res[k] = v / float64(total)
	}
	return
}

type set func(rune) bool

func (a set) add(rt *unicode.RangeTable) set {
	b := in(rt)
	return func(r rune) bool { return a(r) || b(r) }
}

func (a set) sub(rt *unicode.RangeTable) set {
	b := in(rt)
	return func(r rune) bool { return a(r) && !b(r) }
}

func in(rt *unicode.RangeTable) set {
	return func(r rune) bool { return unicode.Is(rt, r) }
}

var id_continue = set(unicode.IsLetter).
	add(unicode.Nl).
	add(unicode.Other_ID_Start).
	sub(unicode.Pattern_Syntax).
	sub(unicode.Pattern_White_Space).
	add(unicode.Mn).
	add(unicode.Mc).
	add(unicode.Nd).
	add(unicode.Pc).
	add(unicode.Other_ID_Continue).
	sub(unicode.Pattern_Syntax).
	sub(unicode.Pattern_White_Space)

// Continue checks that the rune continues an identifier.
func Continue(r rune) bool {
	// id_continue(r) && NFKC(r) in "id_continue*"
	if !id_continue(r) {
		return false
	}
	for _, r := range norm.NFKC.String(string(r)) {
		if !id_continue(r) {
			return false
		}
	}
	return true
}
