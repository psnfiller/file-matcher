package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"
)

type stats struct {
	readdirs    int
	errors      int
	files       int
	bytes       int64
	hashes      int
	bytesHashed int64
}

func printStats() {
	fmt.Println("stats")
	fmt.Printf("dirs %d\n", st.readdirs)
	fmt.Printf("errors %d\n", st.errors)
	fmt.Printf("files %d\n", st.files)
	fmt.Printf("hashes %d\n", st.hashes)
	fmt.Printf("bytes %d\n", st.bytes)
	fmt.Printf("bytes hashed %d\n", st.bytesHashed)
}

type file struct {
	fi   os.FileInfo
	path string
}

func (f file) Size() int64 { return f.fi.Size() }

func processDir(dir string, stat *stats) ([]file, error) {
	fi, err := ioutil.ReadDir(dir)
	stat.readdirs++
	if err != nil {
		stat.errors++
		return []file{}, err
	}
	out := make([]file, 0, len(fi))
	for _, e := range fi {
		p := path.Join(dir, e.Name())
		if e.IsDir() {
			x, err := processDir(p, stat)
			out = append(out, x...)
			if err != nil {
				log.Print(err)
			}
		} else if e.Mode().IsRegular() && e.Size() > 0 {
			stat.files++
			stat.bytes += e.Size()
			x := file{e, p}
			out = append(out, x)
		}

	}
	return out, nil
}

func hashFiles(in []file, stat *stats) ([]file, error) {
	hashes := make(map[string][]file)
	for _, fi := range in {
		f, err := os.Open(fi.path)
		if err != nil {
			return []file{}, err
		}
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			return []file{}, err
		}
		f.Close()
		key := fmt.Sprintf("%x", h.Sum(nil))
		stat.hashes++
		hashes[key] = append(hashes[key], fi)
		stat.bytes_hashed += fi.fi.Size()
	}
	for _, v := range hashes {
		if len(v) > 1 {
			return v, nil
		}
	}
	return []file{}, nil
}

func main() {
	dir := os.Args[1]
	st := stats{}
	start := time.Now()
	fi, err := processDir(dir, &st)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(time.Since(start))
	fmt.Println("stats")
	fmt.Printf("dirs %d\n", st.readdirs)
	fmt.Printf("errors %d\n", st.errors)
	fmt.Printf("files %d\n", st.files)

	sizeToFiles := make(map[int64][]file)
	for _, e := range fi {
		sizeToFiles[e.Size()] = append(sizeToFiles[e.Size()], e)
	}
	start = time.Now()
	for _, v := range sizeToFiles {
		if len(v) <= 2 {
			continue
		}
		_, err := hashFiles(v, &st)
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println(time.Since(start))
	printStats()
}
