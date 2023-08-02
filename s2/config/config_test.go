package config

import (
	"fmt"
	"testing"
)

func TestConfig(t *testing.T) {
	fmt.Println(Save("test/1", map[string]any{"a": 1, "@": 4}))
	x := make(map[string]any)
	Load("test/1", &x)
	fmt.Println(x)
}
