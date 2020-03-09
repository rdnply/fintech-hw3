package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

func readCSVFile(fileName string) (chan string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("unable to read input file %s, %v", fileName, err)
	}

	records := make(chan string)

	go func() {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			records <- scanner.Text()
		}
		close(records)
	}()

	return records, nil
}


func main() {
	records, err := readCSVFile("trades.csv")
	if err != nil {
		log.Fatal(err)
	}
}
