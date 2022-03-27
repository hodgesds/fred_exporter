package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/time/rate"
)

const (
	basePath             = "https://api.stlouisfed.org"
	observationsPath     = "fred/series/observations"
	seriesMetaPath       = "fred/series"
	namespace            = "fred"
	exporter             = "fred_exporter"
	eventConfigNamespace = "events"
)

func isLeapYear(year int) bool {
	leapFlag := false
	if year%4 == 0 {
		if year%100 == 0 {
			if year%400 == 0 {
				leapFlag = true
			} else {
				leapFlag = false
			}
		} else {
			leapFlag = true
		}
	} else {
		leapFlag = false
	}
	return leapFlag
}

type arrayFlags []string

func (i *arrayFlags) String() string {
	return "my string representation"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var (
	seriesFlags arrayFlags
	port        int
	apiKey      string
)

func init() {
	flag.Var(&seriesFlags, "series", "FRED series ID")
	flag.IntVar(&port, "port", 9170, "HTTP port")
	flag.StringVar(&apiKey, "key", "FRED_API_KEY", "FRED API key")
}

type Observation struct {
	RealtimeStart time.Time `json:"realtime_start"`
	RealtimeEnd   time.Time `json:"realtime_end"`
	Date          time.Time `json:"date"`
	Value         float64   `json:"value"`
}

func (o *Observation) UnmarshalJSON(b []byte) error {
	s := struct {
		RealtimeStart string `json:"realtime_start"`
		RealtimeEnd   string `json:"realtime_end"`
		Date          string `json:"date"`
		Value         string `json:"value"`
	}{}
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	t, err := time.Parse("2006-01-02", s.RealtimeStart)
	if err != nil {
		return err
	}
	o.RealtimeStart = t
	t, err = time.Parse("2006-01-02", s.RealtimeEnd)
	if err != nil {
		return err
	}
	o.RealtimeEnd = t
	t, err = time.Parse("2006-01-02", s.Date)
	if err != nil {
		return err
	}
	o.Date = t

	if s.Value == "." {
		return nil
	}
	f, err := strconv.ParseFloat(s.Value, 64)
	if err != nil {
		return err
	}
	o.Value = f
	return nil
}

type SeriesResponse struct {
	RealtimeStart    time.Time `json:"realtime_start"`
	RealtimeEnd      time.Time `json:"realtime_end"`
	ObservationStart time.Time `json:"observation_start"`
	ObservationEnd   time.Time `json:"observation_end"`
	Units            string    `json:"units"`
	OutputType       int       `json:"output_type"`
	FileType         string    `json:"file_type"`
	OrderBy          string    `json:"order_by"`
	SortOrder        string    `json:"sort_order"`
	Count            int       `json:"count"`
	Offset           int       `json:"offset"`
	Limit            int       `json:"limit"`
	ErrorCode        int       `json:"error_code"`
	Error            string    `json:"error_code"`
	Observations     []*Observation
}

func (r *SeriesResponse) UnmarshalJSON(b []byte) error {
	s := struct {
		RealtimeStart    string         `json:"realtime_start"`
		RealtimeEnd      string         `json:"realtime_end"`
		ObservationStart string         `json:"observation_start"`
		ObservationEnd   string         `json:"observation_end"`
		Units            string         `json:"units"`
		OutputType       int            `json:"output_type"`
		FileType         string         `json:"file_type"`
		OrderBy          string         `json:"order_by"`
		SortOrder        string         `json:"sort_order"`
		Count            int            `json:"count"`
		Offset           int            `json:"offset"`
		Limit            int            `json:"limit"`
		ErrorCode        int            `json:"error_code"`
		Error            string         `json:"error_code"`
		Observations     []*Observation `json:"observations"`
	}{}
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	if s.RealtimeStart != "" {
		t, err := time.Parse("2006-01-02", s.RealtimeStart)
		if err != nil {
			return err
		}
		r.RealtimeStart = t
	}
	if s.RealtimeEnd != "" {
		t, err := time.Parse("2006-01-02", s.RealtimeEnd)
		if err != nil {
			return err
		}
		r.RealtimeEnd = t
	}
	if s.ObservationStart != "" {
		t, err := time.Parse("2006-01-02", s.ObservationStart)
		if err != nil {
			return err
		}
		r.ObservationStart = t
	}
	if s.ObservationEnd != "" {
		t, err := time.Parse("2006-01-02", s.ObservationEnd)
		if err != nil {
			return err
		}
		r.ObservationEnd = t
	}
	r.Units = s.Units
	r.OutputType = s.OutputType
	r.FileType = s.FileType
	r.OrderBy = s.OrderBy
	r.SortOrder = s.SortOrder
	r.Count = s.Count
	r.Offset = s.Offset
	r.Limit = s.Limit
	r.Observations = s.Observations
	r.ErrorCode = s.ErrorCode
	r.Error = s.Error

	return nil
}

type SeriesMetadata struct {
	ID                      string
	RealtimeStart           time.Time
	RealtimeEnd             time.Time
	Title                   string
	ObservationStart        time.Time `json:"observation_start"`
	ObservationEnd          time.Time `json:"observation_end"`
	Frequency               string
	FrequencyShort          string
	Units                   string
	UnitsShort              string
	SeasonalAdjustment      string
	SeasonalAdjustmentShort string
	LastUpdated             time.Time
	Popularity              int
	Notes                   string
	ErrorCode               int    `json:"error_code"`
	Error                   string `json:"error_code"`
}

func (sm *SeriesMetadata) UnmarshalJSON(b []byte) error {
	s := struct {
		ID                      string `json:"id"`
		RealtimeStart           string `json:"realtime_start"`
		RealtimeEnd             string `json:"realtime_end"`
		Title                   string `json:"title"`
		ObservationStart        string `json:"observation_start"`
		ObservationEnd          string `json:"observation_end"`
		Frequency               string `json:"frequency"`
		FrequencyShort          string `json:"frequency_short"`
		Units                   string `json:"units"`
		UnitsShort              string `json:"units_short"`
		SeasonalAdjustment      string `json:"seasonal_adjustment"`
		SeasonalAdjustmentShort string `json:"seasonal_adjustment_short"`
		LastUpdated             string `json:"last_updated"`
		Popularity              int    `json:"popularity"`
		Notes                   string `json:"notes"`
		ErrorCode               int    `json:"error_code"`
		Error                   string `json:"error_code"`
	}{}
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	sm.Title = s.Title
	sm.Frequency = s.Frequency
	sm.FrequencyShort = s.FrequencyShort
	sm.Units = s.Units
	sm.UnitsShort = s.UnitsShort
	sm.SeasonalAdjustment = s.SeasonalAdjustment
	sm.SeasonalAdjustmentShort = s.SeasonalAdjustmentShort
	sm.Popularity = s.Popularity
	sm.Notes = s.Notes
	sm.Error = s.Error
	sm.ErrorCode = s.ErrorCode
	if s.RealtimeStart != "" {
		t, err := time.Parse("2006-01-02", s.RealtimeStart)
		if err != nil {
			return err
		}
		sm.RealtimeStart = t
	}
	if s.RealtimeEnd != "" {
		t, err := time.Parse("2006-01-02", s.RealtimeEnd)
		if err != nil {
			return err
		}
		sm.RealtimeEnd = t
	}
	if s.ObservationStart != "" {
		t, err := time.Parse("2006-01-02", s.ObservationStart)
		if err != nil {
			return err
		}
		sm.ObservationStart = t
	}
	if s.ObservationEnd != "" {
		t, err := time.Parse("2006-01-02", s.ObservationEnd)
		if err != nil {
			return err
		}
		sm.ObservationEnd = t
	}

	if s.LastUpdated != "" {
		t, err := time.Parse("2006-01-02 15:04:05-07", s.LastUpdated)
		if err != nil {
			return err
		}
		sm.LastUpdated = t
	}
	return nil
}

type SeriesMetadataResponse struct {
	RealtimeStart time.Time
	RealtimeEnd   time.Time
	ErrorCode     int    `json:"error_code"`
	Error         string `json:"error_code"`
	Series        []*SeriesMetadata
}

func (r *SeriesMetadataResponse) UnmarshalJSON(b []byte) error {
	s := struct {
		RealtimeStart string            `json:"realtime_start"`
		RealtimeEnd   string            `json:"realtime_end"`
		ErrorCode     int               `json:"error_code"`
		Error         string            `json:"error_code"`
		Series        []*SeriesMetadata `json:"seriess"` //WTF
	}{}
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	r.Error = s.Error
	r.ErrorCode = s.ErrorCode
	if s.RealtimeStart != "" {
		t, err := time.Parse("2006-01-02", s.RealtimeStart)
		if err != nil {
			return err
		}
		r.RealtimeStart = t
	}
	if s.RealtimeEnd != "" {
		t, err := time.Parse("2006-01-02", s.RealtimeEnd)
		if err != nil {
			return err
		}
		r.RealtimeEnd = t
	}
	r.Series = s.Series
	return nil
}

// FredCollector is an interface that embeds the prometheus Collector interface.
type FredCollector interface {
	prometheus.Collector
}

type collectorSeries struct {
	desc       *prometheus.Desc
	title      string
	lastUpdate time.Time
	value      float64
	frequency  string
}

func (cs *collectorSeries) shouldUpdate() bool {
	now := time.Now()
	switch cs.frequency {
	case "d", "D", "Daily":
		return now.After(cs.lastUpdate.Add(24 * time.Hour))
	case "w", "W", "Weekly":
		return now.After(cs.lastUpdate.Add(7 * 24 * time.Hour))
	case "bw", "BW", "Biweekly":
		return now.After(cs.lastUpdate.Add(14 * 24 * time.Hour))
	case "m", "M", "Monthly":
		switch now.Month() {
		case time.January, time.March, time.May, time.July, time.August, time.October, time.December:
			return now.After(cs.lastUpdate.Add(31 * 24 * time.Hour))
		case time.April, time.June, time.September, time.November:
			return now.After(cs.lastUpdate.Add(30 * 24 * time.Hour))
		case time.February:
			if isLeapYear(now.Year()) {
				return now.After(cs.lastUpdate.Add(28 * 24 * time.Hour))
			} else {
				return now.After(cs.lastUpdate.Add(29 * 24 * time.Hour))
			}
		default:
			return true
		}
	case "q", "Q", "Quarterly":
		return now.After(cs.lastUpdate.Add(91 * 24 * time.Hour))
	case "sa", "SA", "Semiannual":
		return now.After(cs.lastUpdate.Add(182 * 24 * time.Hour))
	case "a", "A", "Annual":
		return now.After(cs.lastUpdate.Add(365 * 24 * time.Hour))
	default:
		return true
	}
	return true
}

type fredCollector struct {
	client   *http.Client
	seriesMu sync.RWMutex
	apiKey   string
	series   map[string]*collectorSeries
}

// NewFredCollector returns a new PerfCollector.
func NewFredCollector(apiKey string, series []string) (FredCollector, error) {
	c := http.DefaultClient
	c.Transport = NewRateLimitedRoundTripper(http.DefaultTransport, rate.Limit(1), 3)

	collector := &fredCollector{
		client: c,
		apiKey: apiKey,
		series: make(map[string]*collectorSeries),
	}
	desc := prometheus.NewDesc(
		prometheus.BuildFQName(
			namespace,
			"series",
			"values",
			//strings.ToLower(s),
		),
		"FRED time series",
		[]string{"series"},
		nil,
	)
	for _, s := range series {
		// Attempt to retrieve the series to get name/metadata
		seriesInfo := strings.ToLower(s)
		meta, err := collector.getMeta(s)
		if meta.Error != "" {
			println(meta.Error)
			continue
		}
		frequency := ""
		if err == nil {
			if len(meta.Series) > 0 {
				seriesInfo = meta.Series[0].Title
				frequency = meta.Series[0].FrequencyShort
			}
		} else {
			println(err.Error())
		}
		collector.series[s] = &collectorSeries{
			desc:      desc,
			title:     seriesInfo,
			frequency: frequency,
		}
	}

	return collector, nil
}

func (c *fredCollector) seriesName(series string) (string, error) {
	return "", nil
}

// Describe implements the prometheus.Collector interface.
func (c *fredCollector) Describe(ch chan<- *prometheus.Desc) {
	c.seriesMu.RLock()
	for _, cs := range c.series {
		ch <- cs.desc
	}
	c.seriesMu.RUnlock()
}

func (c *fredCollector) getSeries(name string) (*SeriesResponse, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("%s/%s", basePath, observationsPath),
		nil,
	)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Add("series_id", name)
	q.Add("api_key", c.apiKey)
	q.Add("file_type", "json")
	req.URL.RawQuery = q.Encode()
	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	buf, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	series := &SeriesResponse{}
	if err := json.Unmarshal(buf, series); err != nil {
		return nil, err
	}
	return series, nil
}

func (c *fredCollector) getMeta(name string) (*SeriesMetadataResponse, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("%s/%s", basePath, seriesMetaPath),
		nil,
	)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Add("series_id", name)
	q.Add("api_key", c.apiKey)
	q.Add("file_type", "json")
	req.URL.RawQuery = q.Encode()
	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	buf, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	meta := &SeriesMetadataResponse{}
	if err := json.Unmarshal(buf, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// Collect implements prometheus.Collector interface.
func (c *fredCollector) Collect(ch chan<- prometheus.Metric) {
	c.seriesMu.RLock()
	for series, cs := range c.series {
		n := metricName(series)
		//if cs.shouldUpdate() {
			res, err := c.getSeries(series)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if res.Error != "" {
				fmt.Println(res.Error)
				cs.lastUpdate = time.Now()
				continue
			}
			if len(res.Observations) > 0 {
				cs.value = res.Observations[len(res.Observations)-1].Value
				cs.lastUpdate = time.Now()
			}
		//}
		ch <- prometheus.MustNewConstMetric(
			cs.desc,
			prometheus.GaugeValue,
			cs.value,
			n,
		)
	}
	c.seriesMu.RUnlock()
}

func metricName(s string) string {
	return strings.ToLower(strings.Replace(strings.Replace(s, ":", "_", -1), "-", "_", -1))
}

func main() {
	flag.Parse()
	collector, err := NewFredCollector(
		apiKey,
		seriesFlags,
	)
	if err != nil {
		log.Fatal(err)
	}

	prometheus.MustRegister(collector)

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>FRED Exporter</title></head>
             <body>
             <h1>FRED Exporter</h1>
             <p><a href=/metrics>Metrics</a></p>
             </body>
             </html>`))
	})
	err = http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		log.Fatal(err)
	}

}
