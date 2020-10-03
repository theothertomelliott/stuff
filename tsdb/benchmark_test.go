package tsdb_test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/jjneely/stuff/tsdb"
	"github.com/prometheus/tsdb/labels"
)

func BenchmarkOneHourOfData(b *testing.B) {
	duration := time.Hour
	endTime := time.Now()
	startTime := endTime.Add(-duration)

	totalSeries := 3
	timeSeriesStartIndex := 0
	timeSeries := 3

	for i := 0; i < b.N; i++ {
		// TODO: Clean up
		dataDir := fmt.Sprintf("testdata/data-%v", i)

		var generators []tsdb.TimeseriesGenerator
		// Note that 0-padding ensures sorted ordering
		nameTmpl := fmt.Sprintf("test-metric-%%0%dd",
			int(math.Ceil(math.Log10(float64(totalSeries)))))
		for i := 0; i < timeSeries; i++ {
			generators = append(
				generators,
				tsdb.NewIncreasingTimeseriesGenerator(
					fmt.Sprintf("test%d", i),
					labels.Labels{
						labels.Label{
							Name:  "instance",
							Value: fmt.Sprintf(nameTmpl, i+timeSeriesStartIndex),
						},
					},
					startTime,
				),
			)
		}

		err := tsdb.CreateThanosTSDB(tsdb.Opts{
			OutputDir:          dataDir,
			Timeseries:         generators,
			TotalNumTimeSeries: totalSeries,
			StartTime:          startTime,
			EndTime:            endTime,
			SampleInterval:     time.Minute,
			BlockLength:        2 * time.Hour,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
