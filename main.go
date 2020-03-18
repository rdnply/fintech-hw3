package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
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

func readCSVFile(ctx context.Context, fileName string, wg *sync.WaitGroup) (chan string, chan error, error) {
	defer wg.Done()
	file, err := os.Open(fileName)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to read input file %s, %v", fileName, err)
	}

	out := make(chan string)
	errc := make(chan error)

	go func() {
		defer file.Close()
		defer close(out)
		defer close(errc)
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			select {
			case out <- scanner.Text():
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			errc <- fmt.Errorf("unable to parse record from file %s, %v", fileName, err)
			return
		}
	}()

	return out, errc, nil
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

func write(ctx context.Context, in chan Candle, names []string, wg *sync.WaitGroup) (chan error, error) {
	files, err := makeFiles(names)
	if err != nil {
		return nil, err
	}

	errc := make(chan error)
	go func() {
		defer wg.Done()
		defer closeFiles(files)
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
			fmt.Println(s)

			err := w.Write(s)
			if err != nil {
				errc <- fmt.Errorf("trouble with writing string in file: %v", err)
				return
			}
			w.Flush()

			select {
			case <-ctx.Done():
				return
			default:
				continue
			}
		}
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

func handleCandle(d time.Duration) (func(c Candle, out chan Candle, doneFunc chan bool), error) {
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

	return func(c Candle, out chan Candle, doneFunc chan bool) {
		go func() {
			if c.ticker != "" {
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
			doneFunc <- true
		}()

	}, nil
}

func processing(ctx context.Context, in chan string, wg *sync.WaitGroup) (chan Candle, chan error, error) {
	defer wg.Done()

	out := make(chan Candle)
	errc := make(chan error)
	done := make(chan bool)

	handlers := make([]func(c Candle, out chan Candle, done chan bool), 0)
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

	handlers = append(handlers, h5)
	handlers = append(handlers, h30)
	handlers = append(handlers, h240)

	go func() {
		defer close(errc)
		defer close(out)

		for rec := range in {
			c, err := makeCandle(rec)
			if err != nil {
				errc <- err
				return
			}
			if inWorkInterval(c.time) {
				for _, f := range handlers {
					f(c, out, done)
					<-done
				}
			}
			select {
			case <-ctx.Done():
				return
			default:
				continue
			}
		}
		for _, f := range handlers {
			f(Candle{}, out, done)
			<-done
		}
	}()

	return out, errc, nil
}

func pipeline(path string) error {
	duration := 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var (
		wg       sync.WaitGroup
		errcList []<-chan error
		names    = []string{"candles_5min.csv", "candles_30min.csv", "candles_240min.csv"}
	)

	wg.Add(1)
	records, errc, err := readCSVFile(ctx, path, &wg)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	wg.Add(1)
	out, errc, err := processing(ctx, records, &wg)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)
	wg.Add(1)
	errc, err = write(ctx, out, names, &wg)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)
	wg.Wait()

	if ctx.Err() != nil {
		return fmt.Errorf("%v with timeout %v", ctx.Err(), duration)
	}

	return checkErrors(errcList)
}

func checkErrors(errcList []<-chan error) error {
	for _, errc := range errcList {
		for err := range errc {
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func main() {
	var file = flag.String("file", "", "The path for file which contains trades")
	flag.Parse()

	err := pipeline(*file)
	if err != nil {
		log.Fatal(err)
	}
}
