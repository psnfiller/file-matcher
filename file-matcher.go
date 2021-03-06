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

	"net/http"
	_ "net/http/pprof"
)

var (
	cpuprofile     = flag.String("cpuprofile", "", "write cpu profile to file")
	statTimer      = flag.Duration("time_for_stats", time.Second*60, "")
	readdirWorkers = flag.Int("readdir_workers", 10, "")
	port           = flag.Int("port", 8080, "")
)

type stats struct {
	mu               sync.Mutex
	readdirs         int
	errors           int
	files            int
	bytes            int64
	hashes           int
	shortHashes      int
	bytesHashed      int64
	shortBytesHashed int
	shortBytesSaving int
	hashStart        time.Time
	hashEnd          time.Time
	readDirStart     time.Time
	readDirEnd       time.Time
	matches          int
	sizeMatches      int
	shortHashMatches int
}

func printStats(st *stats) {
	st.mu.Lock()
	fmt.Println("stats")
	fmt.Printf("dirs %d\n", st.readdirs)
	fmt.Printf("errors %d\n", st.errors)
	fmt.Printf("files %d\n", st.files)
	fmt.Printf("hashes %d\n", st.hashes)
	fmt.Printf("short hashes %d\n", st.shortHashes)
	fmt.Printf("bytes %s\n", humanize.Bytes(uint64(st.bytes)))
	fmt.Printf("bytes short hashed %s\n", humanize.Bytes(uint64(st.shortBytesHashed)))
	fmt.Printf("bytes short saving %s\n", humanize.Bytes(uint64(st.shortBytesSaving)-uint64(st.bytesHashed)))
	fmt.Printf("bytes hashed %s\n", humanize.Bytes(uint64(st.bytesHashed)))
	fmt.Printf("matches %d (%2.0f%%)\n", st.matches, (float64(st.matches) / float64(st.files) * 100.0))
	fmt.Printf("size matches %d (%2.0f%%)\n", st.sizeMatches, (float64(st.sizeMatches) / float64(st.files) * 100.0))
	fmt.Printf("short hash matches %d (%2.0f%%)\n", st.shortHashMatches, (float64(st.shortHashMatches) / float64(st.files) * 100.0))
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
	shortHashLength int
}

func (f file) Size() int64 { return f.fi.Size() }

func processDir(dir string, stat *stats) ([]file, error) {
	errors := make(chan error)
	dirs := make(chan string)
	files := make(chan file)
	jobs := make(chan string, *readdirWorkers)
	done := make(chan int, *readdirWorkers)

	// start the workers
	for i := 0; i < *readdirWorkers; i++ {
		go readDirWorker(i, jobs, dirs, files, done, errors)
	}

	// first dir
	jobs <- dir
	stat.mu.Lock()
	stat.readdirs++
	stat.mu.Unlock()

	outstanding := 1
	var out []file
	var buffer []string
	for {
		select {
		case err := <-errors:
			log.Print(err)
			stat.mu.Lock()
			stat.errors++
			stat.mu.Unlock()
		case d := <-dirs:
			stat.mu.Lock()
			stat.readdirs++
			stat.mu.Unlock()
			buffer = append(buffer, d)
		case <-done:
			outstanding--
		case f := <-files:
			stat.mu.Lock()
			stat.files++
			stat.bytes += f.Size()
			stat.mu.Unlock()
			out = append(out, f)
			continue
		}
		mark := len(buffer)
		for i, d := range buffer {
			var full bool
			select {
			case jobs <- d:
				outstanding++
			default:
				full = true
			}
			if full {
				mark = i
				break
			}
		}

		if len(buffer) > 0 {
			buffer = buffer[mark:]
		}

		if outstanding == 0 {
			break
		}
	}
	close(jobs)
	close(dirs)
	close(files)
	return out, nil
}

func readDirWorker(id int, jobs <-chan string, dirs chan<- string, fileChan chan<- file, done chan<- int, errors chan<- error) {
	for dir := range jobs {
		fi, err := ioutil.ReadDir(dir)
		if err != nil {
			errors <- err
			done <- id
			continue
		}
		for _, e := range fi {
			p := path.Join(dir, e.Name())
			if e.IsDir() {
				dirs <- p
			} else if e.Mode().IsRegular() && e.Size() > 0 {
				x := file{}
				x.fi = e
				x.path = p
				fileChan <- x
			}
		}
		done <- id
	}
}

func shortHashWorker(id int, wg *sync.WaitGroup, jobs <-chan file, results chan<- file, stat *stats) {
	bufferSize := 4 << 20
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

		bytesWritten, err := h.Write(buffer[:bytesRead])
		if err != nil || bytesWritten != bytesRead {
			if err != nil {
				log.Print(err)
			} else {
				log.Printf("bytes written != bytes read %d %d", bytesWritten, bytesRead)
			}
			stat.mu.Lock()
			stat.errors++
			err = f.Close()
			if err != nil {
				log.Print(err)
			}
			stat.mu.Unlock()
			continue
		}
		err = f.Close()
		if err != nil {
			log.Print(err)
		}
		key := fmt.Sprintf("%x", h.Sum(nil))
		stat.mu.Lock()
		stat.shortHashes++
		stat.shortBytesHashed += bytesRead
		stat.shortBytesSaving += (int(fi.Size()) - bytesRead)
		stat.mu.Unlock()
		out := file{}
		out.fi = fi.fi
		out.path = fi.path
		out.hash = key
		out.shortHashLength = bytesRead
		results <- out
	}
	wg.Done()
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
		if err := f.Close(); err != nil {
			log.Print(err)
		}
		key := fmt.Sprintf("%x", h.Sum(nil))
		stat.mu.Lock()
		stat.hashes++
		stat.bytesHashed += fi.fi.Size()
		stat.mu.Unlock()
		out := file{}
		out.fi = fi.fi
		out.path = fi.path
		out.hash = key
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
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Print(err)
		}
		defer pprof.StopCPUProfile()
	}
	if *port > 0 {
		go func() {
			addr := fmt.Sprintf(":%d", *port)
			log.Fatal(http.ListenAndServe(addr, nil))
		}()
	}
	st := stats{}
	go func() {
		for {
			time.Sleep(*statTimer)
			printStats(&st)
		}
	}()
	findMatchingFiles(flag.Args()[0], &st)
}

type worker func(int, *sync.WaitGroup, <-chan file, chan<- file, *stats)

func findMatchingFilesByHash(files []file, st *stats, workerFunc worker) [][]file {
	jobs := make(chan file, 100)
	results := make(chan file, 100)
	workers := 50
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go workerFunc(w, &wg, jobs, results, st)
	}
	go func() {
		for _, v := range files {
			jobs <- v
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
	out := make([][]file, 0, 0)
	for _, v := range hashToFiles {
		if len(v) > 1 {
			out = append(out, v)
		}
	}
	return out
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
	files := make([]file, 0)
	for _, v := range sizeToFiles {
		if len(v) > 1 {
			st.sizeMatches += len(v)
			files = append(files, v...)
		}
	}
	st.hashStart = time.Now()
	shortFiles := findMatchingFilesByHash(files, st, shortHashWorker)
	files = make([]file, 0)
	for _, v := range shortFiles {
		if len(v) > 1 {
			st.shortHashMatches += len(v)
			files = append(files, v...)
		}
	}
	matches := findMatchingFilesByHash(files, st, hashWorker)
	for _, v := range matches {
		st.matches += len(v)
		fmt.Println(v)
	}

	printStats(st)
}
