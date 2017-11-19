package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
)

import _ "net/http/pprof"

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

type stats struct {
	mu          sync.Mutex
	readdirs    int
	errors      int
	files       int
	bytes       int64
	hashes      int
	bytesHashed int64
	hashStart   time.Time
}

func printStats(st *stats) {

	st.mu.Lock()
	fmt.Println("stats")
	fmt.Printf("dirs %d\n", st.readdirs)
	fmt.Printf("errors %d\n", st.errors)
	fmt.Printf("files %d\n", st.files)
	fmt.Printf("hashes %d\n", st.hashes)
	fmt.Printf("bytes %s\n", humanize.Bytes(uint64(st.bytes)))
	fmt.Printf("bytes hashed %s\n", humanize.Bytes(uint64(st.bytesHashed)))
	if !st.hashStart.IsZero() {
		secs := time.Since(st.hashStart).Seconds()
		throughput := float64(st.bytesHashed) / float64(secs)
		v, unit := humanize.ComputeSI(throughput)

		fmt.Printf("hash throughput %.2f%sBytes/sec\n", v, unit)
	}
	st.mu.Unlock()
}

type file struct {
	fi   os.FileInfo
	path string
	hash string
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
			x := file{e, p, ""}
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
			log.Print(err)
			stat.errors++
			continue
		}
		h := sha256.New()

		if _, err := io.Copy(h, f); err != nil {
			log.Print(err)
			stat.errors++
			continue
		}
		f.Close()
		key := fmt.Sprintf("%x", h.Sum(nil))
		stat.hashes++
		hashes[key] = append(hashes[key], fi)
		stat.bytesHashed += fi.fi.Size()
	}

	for _, v := range hashes {
		if len(v) > 1 {
			return v, nil
		}
	}
	return []file{}, nil
}

func hashWorker(id int, wg *sync.WaitGroup, jobs <-chan file, results chan<- file, stat *stats) {
	for fi := range jobs {
		f, err := os.Open(fi.path)
		if err != nil {
			log.Print(err)
			stat.mu.Lock()
			stat.errors++
			stat.mu.Unlock()
			continue
		}

		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			log.Print(err)
			stat.mu.Lock()
			stat.errors++
			stat.mu.Unlock()
			continue
		}
		f.Close()
		key := fmt.Sprintf("%x", h.Sum(nil))
		stat.mu.Lock()
		stat.hashes++
		stat.bytesHashed += fi.fi.Size()
		stat.mu.Unlock()
		out := file{fi.fi, fi.path, key}
		results <- out
	}
	wg.Done()
}

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	st := stats{}
	go func() {
		for {
			time.Sleep(time.Second)
			printStats(&st)
		}
	}()
	findMatchingFiles(flag.Args()[0], &st)
}

func findMatchingFiles(dir string, st *stats) {
	fi, err := processDir(dir, st)
	if err != nil {
		log.Fatal(err)
	}

	sizeToFiles := make(map[int64][]file)
	for _, e := range fi {
		sizeToFiles[e.Size()] = append(sizeToFiles[e.Size()], e)
	}

	st.hashStart = time.Now()
	jobs := make(chan file, 100)
	results := make(chan file, 100)
	workers := 50
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go hashWorker(w, &wg, jobs, results, st)
	}

	go func() {
		for _, v := range sizeToFiles {
			if len(v) <= 2 {
				continue
			}
			for _, vv := range v {
				jobs <- vv
			}
		}
		close(jobs)
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	hashToFiles := make(map[string][]file)
	for r := range results {
		hashToFiles[r.hash] = append(hashToFiles[r.hash], r)
	}
	for _, v := range hashToFiles {
		if len(v) > 1 {
			fmt.Println(v)
		}
	}
	printStats(st)
}
