package mr

import (
	"os"
	"testing"
)

func TestCreateTemp(t *testing.T) {
	f, err := os.CreateTemp("temp", "testfile")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func TestLogOpen(t *testing.T) {
	_, err := os.Open("aaa")
	if err != nil {
		t.Log(err)
	}
}
