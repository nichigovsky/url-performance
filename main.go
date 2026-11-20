package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Job struct {
	ID 		int
	URL 	string
}

type Result struct {
	Job			Job
	Status		string
	Duration	time.Duration
}

func worker(jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup){
	defer wg.Done()

	for job := range jobs {
		fmt.Printf("Начало выполнения job с id: %d\n", job.ID)
		start := time.Now()

		time.Sleep(time.Duration(rand.Intn(500) + 200) * time.Millisecond)
		fmt.Printf("Завершена обработка job с id: %d\n", job.ID)

		results <- Result{
			Job: job,
			Status: "processed",
			Duration: time.Since(start),
		}
	}
}

func main() {
	rand.Seed(time.Now().Unix())

	var wg sync.WaitGroup

	jobs := make(chan Job, 10)
	results := make(chan Result, 10)

	numWorkers := 5

	urls := []string{
		"https://ya.ru",
		"https://google.com",
		"https://github.com",
		"https://wiki.org",
		"https://vk.com",
		"https://netflix.com",
		"https://mail.ru",
		"https://youtube.com",
		"https://spotify.com",
		"https://profi.ru",
	}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(jobs, results, &wg)
	}
	
	for i, url := range urls {
		jobs <- Job{
			ID: i,
			URL: url,
		}
	}
	close(jobs)

	go func(){
		wg.Wait()
		close(results)
	}()

	finalResults := make([]Result, 0)
	
	var total time.Duration

	for res := range results {
		fmt.Printf(
			"ID: %-10d | URL: %-25s | Время: %v\n",
			res.Job.ID,
			res.Job.URL,
			res.Duration,
		)		

		total += res.Duration
		finalResults = append(finalResults, res)
	}

	fmt.Printf("\n%d jobs выполнены\n", len(finalResults))
	fmt.Printf("Среднее время: %v\n", total/time.Duration(len(finalResults)))
}
