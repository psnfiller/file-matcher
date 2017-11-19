package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"
)

type stats struct {
	readdirs int
	errors   int
	files    int
}

func processDir(dir string, stat *stats) ([]os.FileInfo, error) {
	fi, err := ioutil.ReadDir(dir)
	stat.readdirs++
	if err != nil {
		stat.errors++
		return fi, err
	}
	out := make([]os.FileInfo, 0, len(fi))
	for _, e := range fi {
		stat.files++
		if e.IsDir() {
			p := path.Join(dir, e.Name())
			x, err := processDir(p, stat)
			out = append(out, x...)
			if err != nil {
				log.Print(err)
			}
		} else if e.Mode().IsRegular() && e.Size() > 0 {
			out = append(out, e)
		}

	}
	return out, nil
}

func main() {
	dir := os.Args[1]
	st := stats{}
	start := time.Now()
	fi, err := processDir(dir, &st)
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range fi {
		fmt.Println(e)
	}
	fmt.Println(time.Since(start))
	fmt.Println("stats")
	fmt.Printf("dirs %d\n", st.readdirs)
	fmt.Printf("errors %d\n", st.errors)
	fmt.Printf("files %d\n", st.files)

	sizeToFiles := make(map[int64][]os.FileInfo)
	for _, e := range fi {
		sizeToFiles[e.Size()] = append(sizeToFiles[e.Size()], e)
	}
	for k, v := range sizeToFiles {
		fmt.Println(k, v)
	}

}
