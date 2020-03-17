package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Five = iota
	Thirty
	TwoHForty
	Unknown
)

type Candle struct {
	ticker     string
	openPrice  float64
	maxPrice   float64
	minPrice   float64
	closePrice float64
	time       time.Time
	scale      int
}

func readCSVFile(fileName string, wg *sync.WaitGroup) (chan string, chan error, error) {
	defer wg.Done()
	file, err := os.Open(fileName)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to read input file %s, %v", fileName, err)
	}

	records := make(chan string)
	errc := make(chan error)
	go func() {
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			records <- scanner.Text()
		}

		close(records)

		defer close(errc)
		if err := scanner.Err(); err != nil {
			errc <- fmt.Errorf("unable to parse record from file %s, %v", fileName, err)
			return
		}
	}()

	return records, errc, nil
}

func (c Candle) toSlice() []string {
	s := make([]string, 0)
	s = append(s, c.ticker)
	t := c.time.Format(time.RFC3339)
	s = append(s, t)
	pr := strconv.FormatFloat(c.openPrice, 'f', -1, 64)
	s = append(s, pr)
	pr = strconv.FormatFloat(c.maxPrice, 'f', -1, 64)
	s = append(s, pr)
	pr = strconv.FormatFloat(c.minPrice, 'f', -1, 64)
	s = append(s, pr)
	pr = strconv.FormatFloat(c.closePrice, 'f', -1, 64)
	s = append(s, pr)

	return s
}

func makeCandle(r string) (Candle, error) {
	const (
		Ticker = iota
		Price
		Time   = 3
		Layout = "2006-01-02 15:04:05"
	)

	rec := strings.Split(r, ",")
	pr, err := strconv.ParseFloat(rec[Price], 64)
	if err != nil {
		return Candle{}, fmt.Errorf("can't convert price string into float64: %v", err)
	}

	t, err := time.Parse(Layout, rec[Time])
	if err != nil {
		return Candle{}, fmt.Errorf("can't convert initial time string into Time: %v", err)
	}

	rfc, err := time.Parse(time.RFC3339, t.Format(time.RFC3339))
	if err != nil {
		return Candle{}, fmt.Errorf("can't convert RFC3339 time string into Time: %v", err)
	}

	out := Candle{rec[Ticker], pr, pr, pr, pr, rfc, Unknown}

	return out, nil
}

func makeFiles(names []string) ([]*os.File, error) {
	files := make([]*os.File, 0)
	for _, n := range names {
		f, err := os.OpenFile(n, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("can't open file with name: %s, %v", n, err)
		}
		files = append(files, f)
	}

	return files, nil
}

func closeFiles(files []*os.File) {
	for _, f := range files {
		f.Close()
	}
}

func write(in chan Candle, done chan int, names []string) (chan error, error) {
	files, err := makeFiles(names)
	if err != nil {
		return nil, err
	}

	errc := make(chan error)
	go func() {
		defer close(errc)
		var w *csv.Writer
		for c := range in {
			switch {
			case c.scale == Five:
				w = csv.NewWriter(files[Five])
			case c.scale == Thirty:
				w = csv.NewWriter(files[Thirty])
			case c.scale == TwoHForty:
				w = csv.NewWriter(files[TwoHForty])
			}
			s := c.toSlice()
			//fmt.Println(s)

			err := w.Write(s)
			if err != nil {
				errc <- fmt.Errorf("trouble with writing string in file: %v", err)
				return
			}
			w.Flush()
		}
		closeFiles(files)
		done <- 1
	}()

	return errc, nil
}

func sleep(t time.Time, s time.Time, e time.Time) bool {
	numMin := func(t time.Time) int {
		h, m, _ := t.Clock()
		return h*60 + m
	}
	return numMin(t) >= numMin(s) && numMin(t) < numMin(e)
}

func inWorkInterval(t time.Time) bool {
	var (
		start = time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)
		end   = time.Date(2006, 1, 2, 7, 0, 0, 0, time.UTC)
	)
	return !sleep(t, start, end)
}

func updatePrice(o Candle, n Candle) Candle {
	res := o
	if res.maxPrice < n.openPrice {
		res.maxPrice = n.openPrice
	}

	if res.minPrice > n.openPrice {
		res.minPrice = n.openPrice
	}

	res.closePrice = n.openPrice

	return res
}

func updateCandle(candles map[string]Candle, c Candle) {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()
	if _, ok := candles[c.ticker]; ok {
		candles[c.ticker] = updatePrice(candles[c.ticker], c)
	} else {
		candles[c.ticker] = c
	}
}

func diffMoreThanScale(diff float64, d time.Duration) bool {
	return diff >= d.Minutes() && math.Abs(diff-d.Minutes()) >= d.Minutes()
}

func sendCandles(candles map[string]Candle, st time.Time, sc int, out chan Candle, done chan bool) {
	go func() {
		for t := range candles {
			c := candles[t]
			c.time = st
			c.scale = sc
			out <- c
			delete(candles, t)
		}
		done <- true
	}()

}

func handleCandle(d time.Duration) (func(c Candle, out chan Candle, w *sync.WaitGroup), error) {
	const (
		Start = "2019-01-30T07:00:00Z"
	)

	candles := make(map[string]Candle)
	startTime, err := time.Parse(time.RFC3339, Start)
	if err != nil {
		return nil, fmt.Errorf("can't convert RFC3339 time string into Time: %v", err)
	}

	scales := map[time.Duration]int{
		5 * time.Minute:   Five,
		30 * time.Minute:  Thirty,
		240 * time.Minute: TwoHForty,
	}

	done := make(chan bool)

	return func(c Candle, out chan Candle, w *sync.WaitGroup) {
		go func() {
			defer w.Done()
			if c.ticker != "empty" {
				diff := c.time.Sub(startTime).Minutes()
				if diff >= d.Minutes() {
						sendCandles(candles, startTime, scales[d], out, done)
						<-done
					if diffMoreThanScale(diff, d) {
						startTime = c.time
					} else {
						if inWorkInterval(startTime.Add(d)) {
							startTime = startTime.Add(d)
						} else {
							return
						}
					}
				}
				updateCandle(candles, c)
			} else {
				sendCandles(candles, startTime, scales[d], out, done)
				<-done
			}
		}()

	}, nil
}


func processing(in chan string, wg *sync.WaitGroup) (chan Candle, chan error, error) {
	defer wg.Done()
	var w sync.WaitGroup

	out := make(chan Candle)
	errc := make(chan error)
	done := make(chan bool)

	h5, err := handleCandle(5 * time.Minute)
	if err != nil {
		return nil, nil, err
	}
	h30, err := handleCandle(30 * time.Minute)
	if err != nil {
		return nil, nil, err
	}
	h240, err := handleCandle(240 * time.Minute)
	if err != nil {
		return nil, nil, err
	}

	go func() {
		defer close(errc)

		for rec := range in {
			c, err := makeCandle(rec)
			if err != nil {
				errc <- err
				return
			}
			if inWorkInterval(c.time) {
				w.Add(1)
				h5(c, out, &w)
				w.Wait()
				w.Add(1)
				h30(c, out, &w)
				w.Wait()
				w.Add(1)
				h240(c, out, &w)
				w.Wait()
			}
		}
		w.Add(1)
		h5(Candle{ticker:"empty"}, out, &w)
		w.Wait()
		w.Add(1)
		h30(Candle{ticker:"empty"}, out, &w)
		w.Wait()
		w.Add(1)
		h240(Candle{ticker:"empty"}, out, &w)
		w.Wait()
		close(out)
	}()

	return out, nil, nil
}

func main() {
	var (
		wg       sync.WaitGroup
		errcList []<-chan error
	)

	wg.Add(1)
	records, errc, err := readCSVFile("trades.csv", &wg)
	if err != nil {
		log.Fatal(err)
	}
	errcList = append(errcList, errc)

	wg.Add(1)
	out, errc, err := processing(records, &wg)
	if err != nil {
		log.Fatal(err)
	}
	errcList = append(errcList, errc)
	done := make(chan int)
	names := []string{"candles5.csv", "candles30.csv", "candles240.csv"}
	errc, err = write(out, done, names)
	if err != nil {
		log.Fatal(err)
	}
	errcList = append(errcList, errc)

	wg.Wait()
	<-done
}
