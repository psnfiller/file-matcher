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
	mu               sync.Mutex
	readdirs         int
	errors           int
	files            int
	bytes            int64
	hashes           int
	bytesHashed      int64
	bytesShortHashed int64
	hashStart        time.Time
	readDirStart     time.Time
	readDirEnd       time.Time
}

func printStats(st *stats) {
	st.mu.Lock()
	fmt.Println("stats")
	fmt.Printf("dirs %d\n", st.readdirs)
	fmt.Printf("errors %d\n", st.errors)
	fmt.Printf("files %d\n", st.files)
	fmt.Printf("hashes %d\n", st.hashes)
	fmt.Printf("bytes %s\n", humanize.Bytes(uint64(st.bytes)))
	fmt.Printf("bytes short hashed %s\n", humanize.Bytes(uint64(st.bytesShortHashed)))
	fmt.Printf("bytes hashed %s\n", humanize.Bytes(uint64(st.bytesHashed)))
	if !st.hashStart.IsZero() {
		secs := time.Since(st.hashStart).Seconds()
		throughput := float64(st.bytesHashed) / float64(secs)
		v, unit := humanize.ComputeSI(throughput)
		fmt.Printf("hash throughput %.2f%sBytes/sec\n", v, unit)
	}
	if !st.readDirStart.IsZero() {
		var secs float64
		if st.readDirEnd.IsZero() {
			secs = time.Since(st.readDirStart).Seconds()
		} else {
			secs = st.readDirEnd.Sub(st.readDirStart).Seconds()
		}
		throughput := float64(st.files) / float64(secs)
		v, unit := humanize.ComputeSI(throughput)
		fmt.Printf("readdir throughput %.2f%sfiles/sec\n", v, unit)
	}

	st.mu.Unlock()
}

type file struct {
	fi              os.FileInfo
	path            string
	hash            string
	shortHash       string
	shortHashLength uint64
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

func shortHashWorker(id int, wg *sync.WaitGroup, jobs <-chan file, results chan<- file, stat *stats, fullFile bool) {
	bufferSize := 4 << 10
	buffer := make([]byte, bufferSize)
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
		bytesRead, err := f.Read(buffer)
		if err != nil {
			log.Print(err)
			stat.mu.Lock()
			stat.errors++
			stat.mu.Unlock()
			continue
		}
		h.Write(buffer)
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

func hashWorker(id int, wg *sync.WaitGroup, jobs <-chan file, results chan<- file, stat *stats, fullFile bool) {
	bufferSize := 4 << 10
	buffer := make([]byte, bufferSize)
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
	st.readDirStart = time.Now()
	fi, err := processDir(dir, st)
	if err != nil {
		log.Fatal(err)
	}
	st.readDirEnd = time.Now()

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
		go shorHashWorker(w, &wg, jobs, results, st)
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
