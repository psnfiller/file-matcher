package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
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

	// single, non-empty, file.
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
	// Clean up tmpfile
	err = os.Remove(name)
	if err != nil {
		t.Errorf("message")
	}

	// single, file.
	f, err = ioutil.TempFile(tmpDir, "f")
	if err != nil {
		t.Errorf("message")
	}
	name = f.Name()
	f.Close()
	files, err = processDir(tmpDir, &st)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(files) != 0 {
		t.Errorf("%v", files)
	}
	// Clean up tmpfile
	err = os.Remove(name)
	if err != nil {
		t.Errorf("message")
	}

	// ten files in the same dir
	var tmpfiles []string
	for i := 0; i < 10; i++ {
		f, err := ioutil.TempFile(tmpDir, "f")
		if err != nil {
			t.Errorf("message")
		}
		f.WriteString("8")
		f.Close()
		tmpfiles = append(tmpfiles, f.Name())
	}
	files, err = processDir(tmpDir, &st)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(files) != 10 {
		t.Errorf("%v", files)
	}

	// cleanup

	for _, f := range tmpfiles {
		if err := os.Remove(f); err != nil {
			t.Errorf("remove file, %s", err)
		}
	}

	// sub dir

	d := path.Join(tmpDir, "d")
	err = os.Mkdir(d, os.FileMode(0766))
	if err != nil {
		t.Errorf("message")
	}

	// ten files in subdir.
	for i := 0; i < 1000; i++ {
		f, err := ioutil.TempFile(d, "f")
		if err != nil {
			t.Errorf("tmpfile %s", err)
		}
		f.WriteString("8")
		f.Close()
	}
	files, err = processDir(tmpDir, &st)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(files) != 1000 {
		t.Errorf("%v", files)
	}

	// cleanup
	err = os.RemoveAll(tmpDir)
	if err != nil {
		t.Errorf("message")
	}
}
func TestProcessDirError(t *testing.T) {
	st := stats{}
	tmpDir, err := ioutil.TempDir(os.Getenv("TMPDIR"), "file-matcher-test")
	if err != nil {
		t.Errorf("%v", err)
	}
	f, err := ioutil.TempFile(tmpDir, "f")
	if err != nil {
		t.Errorf("tmpfile %s", err)
	}
	os.Chmod(f.Name(), 0000)
	f.WriteString("8")
	f.Close()
	if err != nil {
		t.Errorf("chmod %s", err)
	}

	files, err := processDir(tmpDir, &st)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(files) != 0 {
		t.Errorf("files: expected %d, got %d", 0, len(files))
	}
	if st.errors != 1 {
		t.Errorf("errors")
	}

	// cleanup
	err = os.RemoveAll(tmpDir)
	if err != nil {
		t.Errorf("message")
	}
}
func TestProcessDirStress(t *testing.T) {
	st := stats{}
	tmpDir, err := ioutil.TempDir(os.Getenv("TMPDIR"), "file-matcher-test")
	fmt.Println(tmpDir)
	if err != nil {
		t.Errorf("%v", err)
	}
	// ten files in subdir.
	for i := 0; i < 100; i++ {
		dname := fmt.Sprintf("d%d", i)
		d := path.Join(tmpDir, dname)
		err = os.Mkdir(d, os.FileMode(0766))
		if err != nil {
			t.Errorf("failed to createdir %v", err)
		}
		// ten files in subdir.
		for j := 0; j < 100; j++ {
			f, err := ioutil.TempFile(d, "f")
			if err != nil {
				t.Errorf("tmpfile %s", err)
			}
			f.WriteString("8")
			f.Close()
		}
	}
	files, err := processDir(tmpDir, &st)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(files) != 100*100 {
		t.Errorf("expected %d, got %d", 100*100, len(files))
	}
	// cleanup
	err = os.RemoveAll(tmpDir)
	if err != nil {
		t.Errorf("message")
	}
}
