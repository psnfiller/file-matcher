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
	want.readdirs = 1
	if st != want {
		t.Errorf("%v", st)
	}

	f, err := ioutil.TempFile(tmpDir, "f")
	if err != nil {
		t.Errorf("message")
	}
	name := f.Name()
	f.WriteString("8")
	f.Close()
	files, err = processDir(tmpDir, &st)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(files) != 1 {
		t.Errorf("%v", files)
	}
	if files[0].path != name {
		t.Errorf("%s", files[0].path)
	}

	// cleanup
	err = os.RemoveAll(tmpDir)
	if err != nil {
		t.Errorf("message")
	}
}
