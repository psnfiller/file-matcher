package main

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestProcessDir(t *testing.T) {
	st := stats{}
	tmpDir, err := ioutil.TempDir(os.Getenv("TMPDIR"), "file-matcher-test")
	if err != nil {
		t.Errorf("%v", err)
	}
	// empty dir
	files, err := processDir(tmpDir, &st)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(files) != 0 {
		t.Errorf("%v", files)
	}
	want := stats{}
	if st != want {
		t.Errorf("%v", st)
	}
}
