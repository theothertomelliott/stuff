package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/jjneely/stuff/tsdb"
	"github.com/prometheus/tsdb/labels"
)

var (
	duration = flag.Duration("d", time.Hour*720,
		"Time duration of historical data to generate")
	outDir = flag.String("o", "data/",
		"Output directory to generate TSDB blocks in")
	timeSeries = flag.Int("c", 1,
		"Number of time series to generate")
	timeSeriesStartIndex = flag.Int("n", 0,
		"Start index for time series instance names")
	sampleInterval = flag.Duration("i", time.Second*15,
		"Duration between samples")
	blockLength = flag.Duration("b", time.Hour*2,
		"TSDB block length")
)

func main() {
	log.Printf("Generate Prometheus TSDB test data.")
	flag.Parse()

	endTime := time.Now()
	startTime := endTime.Add(-*duration)

	var generators []tsdb.TimeseriesGenerator
	// Note that 0-padding ensures sorted ordering
	nameTmpl := fmt.Sprintf("test-metric-%%0%dd",
		int(math.Ceil(math.Log10(float64(*timeSeries)))))
	for i := 0; i < *timeSeries; i++ {
		generators = append(
			generators,
			tsdb.NewIncreasingTimeseriesGenerator(
				fmt.Sprintf("test%d", i),
				labels.Labels{
					labels.Label{
						Name:  "instance",
						Value: fmt.Sprintf(nameTmpl, i+*timeSeriesStartIndex),
					},
				},
				startTime,
			),
		)
	}

	err := tsdb.CreateThanosTSDB(tsdb.Opts{
		OutputDir:      *outDir,
		Timeseries:     generators,
		StartTime:      startTime,
		EndTime:        endTime,
		SampleInterval: *sampleInterval,
		BlockLength:    *blockLength,
	})

	if err != nil {
		log.Fatalf("Error generating data: %s", err)
	}

	log.Printf("TSDB data generation complete")
}
