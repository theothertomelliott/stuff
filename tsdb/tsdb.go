package tsdb

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
)

const (
	// The amount of overhead data per chunk. This assume 2 bytes to hold data length, 1 byte for version & 4 bytes for
	// CRC hash.
	chunkOverheadSize = 7

	// TSDB enforces that each segment must be at most 512MB.
	maxSegmentSize = 1024 * 1024 * 512

	// Keep chunks small for performance.
	maxChunkSize = 1024 * 16

	// TSDB allows a maximum of 120 samples per chunk.
	samplesPerChunk = 120

	// The size of the header for each segment file.
	segmentStartOffset = 8

	blockMetaTemplate = `{
	"version": 1,
	"ulid": "%s",
	"minTime": %d,
	"maxTime": %d,
	"stats": {
		"numSamples": %d,
		"numSeries": %d,
		"numChunks": %d
	},
	"compaction": {
		"level": 1,
		"sources": [
			"%s"
		]
	}
}`
)

type Opts struct {
	OutputDir      string                // The directory to place the generated TSDB blocks. Default /tmp/tsdb.
	Timeseries     []TimeseriesGenerator // Generators defining each time series to be created.
	StartTime      time.Time             // Metrics will be produced from this time. Default now.
	EndTime        time.Time             // Metrics will be produced until this time. Default 1 week.
	SampleInterval time.Duration         // How often to sample the metrics. Default 15s.
	BlockLength    time.Duration         // The length of time each block will cover. Default 2 hours.
}

type timeseries struct {
	ID     uint64
	Name   string
	Chunks []chunks.Meta
}

func CreateThanosTSDB(opts Opts) error {
	if opts.OutputDir == "" {
		opts.OutputDir = "/tmp/tsdb"
	}

	now := time.Now()
	if opts.StartTime.IsZero() {
		opts.StartTime = now.Add(-time.Hour * 24 * 7)
	}

	if opts.EndTime.IsZero() {
		opts.EndTime = now
	}

	if opts.StartTime.After(opts.EndTime) {
		return errors.New("end time cannot come after start time")
	}

	if opts.SampleInterval == 0 {
		opts.SampleInterval = time.Second * 15
	}

	if opts.BlockLength == 0 {
		opts.BlockLength = time.Hour * 2
	}

	rng := rand.New(rand.NewSource(now.UnixNano()))

	for blockStart := opts.StartTime; blockStart.Before(opts.EndTime); blockStart = blockStart.Add(opts.BlockLength) {
		if err := createBlock(opts, rng, blockStart, blockStart.Add(opts.BlockLength)); err != nil {
			return err
		}
	}

	return nil
}

func createBlock(opts Opts, rng *rand.Rand, blockStart time.Time, blockEnd time.Time) error {
	// Generate block ID.
	blockULID, err := ulid.New(uint64(blockEnd.Unix()), rng)
	if err != nil {
		return errors.Wrap(err, "failed to create ULID for block")
	}
	outputDir := filepath.Join(opts.OutputDir, blockULID.String())

	// Create sorted list of timeseries to write. These will not be populated with data yet.
	series := createEmptyTimeseries(opts.Timeseries)

	// Store chunks in series & write them to disk.
	if err := populateChunks(opts.Timeseries, series, outputDir, blockStart, blockEnd, opts.SampleInterval); err != nil {
		return errors.Wrap(err, "failed to create chunks")
	}

	// Store references to these chunks in the index.
	if err := createIndex(opts.Timeseries, series, outputDir); err != nil {
		return errors.Wrap(err, "failed to create index")
	}

	// Add  metadata for this block.
	numChunks := int64(len(opts.Timeseries)) * (blockEnd.Sub(blockStart).Nanoseconds() / (opts.SampleInterval * samplesPerChunk).Nanoseconds())
	meta := fmt.Sprintf(blockMetaTemplate, blockULID, blockStart.Unix()*1000, blockEnd.Unix()*1000, numChunks*samplesPerChunk, len(opts.Timeseries), numChunks, blockULID)
	if err := ioutil.WriteFile(filepath.Join(outputDir, "meta.json"), []byte(meta), 0755); err != nil {
		return errors.Wrap(err, "failed to write metadata")
	}

	return nil
}

// createEmptyTimeseries will return `numTimeseries` unique timeseries structs. Does not populate these timeseries with
// data yet.
func createEmptyTimeseries(generators []TimeseriesGenerator) []*timeseries {
	series := make([]*timeseries, len(generators))
	for i, gen := range generators {
		series[i] = &timeseries{
			ID:   uint64(i),
			Name: gen.Name(),
		}
	}

	return series
}

// populateChunks will populate `series` with a list of chunks for each timeseries. The chunks will span the entire
// duration from blockStart to blockEnd. It will also write these chunks to the block's output directory.
func populateChunks(generators []TimeseriesGenerator, series []*timeseries, outputDir string, blockStart time.Time, blockEnd time.Time, sampleInterval time.Duration) error {
	cw, err := chunks.NewWriter(filepath.Join(outputDir, "chunks"))
	if err != nil {
		return err
	}

	// The reference into the chunk where a timeseries starts.
	ref := uint64(segmentStartOffset)
	seg := uint64(0)

	// The total size of the chunk.
	chunkLength := sampleInterval * samplesPerChunk

	// Populate each series with fake metrics.
	for i, s := range series {
		// Segment block into small chunks.
		for chunkStart := blockStart; chunkStart.Before(blockEnd); chunkStart = chunkStart.Add(chunkLength) {
			ch := chunkenc.NewXORChunk()
			app, err := ch.Appender()
			if err != nil {
				return err
			}

			// Write series data for this chunk.
			for sample := chunkStart; sample.Before(chunkStart.Add(chunkLength)); sample = sample.Add(sampleInterval) {
				t := sample.Unix() * 1000
				app.Append(t, generators[i].Value(t))
			}

			// Calcuate size of this chunk. This is the amount of bytes written plus the chunk overhead. See
			// https://github.com/prometheus/tsdb/blob/master/docs/format/chunks.md for a breakdown of the overhead.
			// Assumes that the len uvarint has size 2.
			size := uint64(len(ch.Bytes())) + chunkOverheadSize
			if size > maxChunkSize {
				return errors.Errorf("chunk too big, calculated size %d > %d", size, maxChunkSize)
			}

			// Reference a new segment if the current is out of space.
			if ref+size > maxSegmentSize {
				ref = segmentStartOffset
				seg++
			}

			chunkStartMs := chunkStart.Unix() * 1000
			cm := chunks.Meta{
				Chunk:   ch,
				MinTime: chunkStartMs,
				MaxTime: chunkStartMs + sampleInterval.Nanoseconds()/(1000*1000),
				Ref:     ref | (seg << 32),
			}

			s.Chunks = append(s.Chunks, cm)

			ref += size
		}

		if err := cw.WriteChunks(s.Chunks...); err != nil {
			return err
		}
	}

	if err := cw.Close(); err != nil {
		return err
	}

	return nil
}

// createIndex will write the index file. It should reference the chunks previously created.
func createIndex(generators []TimeseriesGenerator, series []*timeseries, outputDir string) error {
	iw, err := index.NewWriter(filepath.Join(outputDir, "index"))
	if err != nil {
		return err
	}

	var labelValues = make(map[string][]string)
	for _, gen := range generators {
		for _, l := range gen.Labels() {
			labelValues[l.Name] = append(labelValues[l.Name], l.Value)
		}
	}

	// Add the symbol table from all symbols we use.
	if err := iw.AddSymbols(getSymbols(labelValues)); err != nil {
		return err
	}

	// Add chunk references.
	for i, s := range series {
		if err := iw.AddSeries(s.ID, generators[i].Labels(), s.Chunks...); err != nil {
			return errors.Wrapf(err, "failed to write timeseries for %s", s.Name)
		}
	}

	// Add mapping of label names to label values that we use.
	for name, values := range labelValues {
		if err := iw.WriteLabelIndex([]string{name}, values); err != nil {
			return err
		}
	}

	// Create & populate postings.
	postings := index.NewMemPostings()
	for i, s := range series {
		postings.Add(s.ID, generators[i].Labels())
	}

	// Add references to index for each label name/value pair.
	for _, l := range postings.SortedKeys() {
		if err := iw.WritePostings(l.Name, l.Value, postings.Get(l.Name, l.Value)); err != nil {
			return errors.Wrap(err, "write postings")
		}
	}

	// Output index to file.
	if err := iw.Close(); err != nil {
		return err
	}

	return nil
}

// getSymbols returns a set of symbols that we use in all timeseries labels & values.
func getSymbols(labelValues map[string][]string) map[string]struct{} {
	symbols := map[string]struct{}{}

	for n, values := range labelValues {
		symbols[n] = struct{}{}
		for _, v := range values {
			symbols[v] = struct{}{}
		}
	}

	return symbols
}
