package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
)

type stats struct {
	readdirs int
	errors   int
	files    int
}

func processDir(dir string, stat *stats) ([]os.FileInfo, error) {
	out, err := ioutil.ReadDir(dir)
	stat.readdirs++
	if err != nil {
		stat.errors++
		return out, err
	}
	for _, e := range out {
		stat.files++
		if e.IsDir() {
			x, err := processDir(path.Join(dir, e.Name()))
			out = append(out, x...)
			if err != nil {
				log.Print(err)
			}
		}
	}
	return out, nil
}

func main() {
	dir := os.Args[1]
	st := stats{}
	fi, err := processDir(dir, &st)
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range fi {
		fmt.Println(e)
	}
	fmt.Println(st)
}
