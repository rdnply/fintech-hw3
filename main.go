package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Candle struct {
	ticker string
	price  float64
	time   time.Time
}

func makeCandle(r string) (*Candle, error) {
	const (
		Ticker = iota
		Price
		Time   = 3
		Layout = "2006-01-02 15:04:05"
	)
	rec := strings.Split(r, ",")
	pr, err := strconv.ParseFloat(rec[Price], 64)
	if err != nil {
		return nil, fmt.Errorf("can't convert price string into float64: %v", err)
	}

	t, err := time.Parse(Layout, rec[Time])
	if err != nil {
		return nil, fmt.Errorf("can't convert initial time string into Time: %v", err)
	}

	tt, err := time.Parse(time.RFC3339, t.Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("can't convert RFC3339 time string into Time: %v", err)
	}

	out := &Candle{rec[Ticker], pr, tt}

	fmt.Println(out)
	return out, nil
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

func processing(in chan string, wg *sync.WaitGroup) (chan *Candle, chan error) {
	defer wg.Done()
	//candles := make(map[string]*Candle)
	out := make(chan *Candle)
	errc := make(chan error)
	go func() {
		defer close(errc)

		for rec := range in {
			c, err := makeCandle(rec)
			if err != nil {
				errc <- err
				return
			}
			out <- c
		}
		close(out)
	}()

	return out, nil
}

//func temp(in chan string) chan *Candle {
//	wg.Done()
//
//	out := make(chan *Candle)
//	go func() {
//		for i := range in {
//			c, _ := makeCandle(i)
//			out <- c
//		}
//		close(out)
//	}()
//
//	return out
//}

func main() {
	var wg sync.WaitGroup
	var errcList []<-chan error

	wg.Add(1)
	records, errc, err := readCSVFile("trades.csv", &wg)
	if err != nil {
		log.Fatal(err)
	}
	errcList = append(errcList, errc)

	wg.Add(1)
	out, errc := processing(records, &wg)
	errcList = append(errcList, errc)
	wg.Wait()
	done := make(chan int)
	go func() {
		for i := range out {
			fmt.Println(i)
		}
		done <- 1
	}()
	<-done

}
