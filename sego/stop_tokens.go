package sego

import (
	"bufio"
	"bytes"
	_ "embed"
	"io"
	"strings"
)

//go:embed data/stop_tokens.txt
var stopWordsBytes []byte

type StopTokens struct {
	stopTokens map[string]bool
}

// 从stopTokenFile中读入停用词，一个词一行
// 文档索引建立时会跳过这些停用词
func (st *StopTokens) Init(stopTokenFile []byte) {
	st.stopTokens = make(map[string]bool)

	scanner := bufio.NewScanner(io.MultiReader(
		bytes.NewReader(stopWordsBytes),
		strings.NewReader("\n"),
		bytes.NewReader(stopTokenFile)))
	for scanner.Scan() {
		text := scanner.Text()
		if text != "" {
			st.stopTokens[text] = true
		}
	}

}

func (st *StopTokens) IsStopToken(token string) bool {
	_, found := st.stopTokens[token]
	return found
}

// 释放资源
func (st *StopTokens) Close() {
	st.stopTokens = nil
}
