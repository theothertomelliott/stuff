package tsdb

import (
	"time"

	"github.com/prometheus/tsdb/labels"
)

// timeseriesGenerator creates samples for a time series
type timeseriesGenerator interface {
	Name() string
	Labels() labels.Labels
	Value(t int64) float64
}

type increasingTimeseriesGenerator struct {
	startTime time.Time
	name      string
	labels    labels.Labels
}

func NewIncreasingTimeseriesGenerator(name string, l labels.Labels, start time.Time) timeseriesGenerator {
	labelsWithName := append(l, labels.Label{
		Name:  "__name__",
		Value: name,
	})
	return &increasingTimeseriesGenerator{
		name:      name,
		startTime: start,
		labels:    labelsWithName,
	}
}

func (i *increasingTimeseriesGenerator) Name() string {
	return i.name
}

func (i *increasingTimeseriesGenerator) Labels() labels.Labels {
	return i.labels
}

func (i *increasingTimeseriesGenerator) Value(t int64) float64 {
	return float64((t / 1000) - i.startTime.Unix())
}
