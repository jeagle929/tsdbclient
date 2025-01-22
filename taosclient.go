package tsdbclient

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"path"
	"time"
)

type ContentEncoding string

const (
	DefaultEncoding ContentEncoding = ""
	GzipEncoding    ContentEncoding = "gzip"
)

const (
	WriteDataURL  = "influxdb/v1/write"
	ExecuteSqlURL = "rest/sql"
)

const InfluxTimeFormat = "2006-01-02T15:04:05Z"

// HTTPConfig is the config data needed to create an HTTP Client.
type HTTPConfig struct {
	// Addr should be of the form "http://host:port"
	Addr string

	// Username is the influxdb username, optional.
	Username string

	// Password is the influxdb password, optional.
	Password string

	// UserAgent is the http User Agent, defaults to "TDEngineDBClient".
	UserAgent string

	// Timeout for tdengine writes, defaults to no timeout.
	Timeout time.Duration

	// InsecureSkipVerify gets passed to the http client, if true, it will
	// skip https certificate verification. Defaults to false.
	InsecureSkipVerify bool

	// TLSConfig allows the user to set their own TLS config for the HTTP
	// Client. If set, this option overrides InsecureSkipVerify.
	TLSConfig *tls.Config

	// Proxy configures the Proxy function on the HTTP client.
	Proxy func(req *http.Request) (*url.URL, error)

	// WriteEncoding specifies the encoding of write request
	WriteEncoding ContentEncoding
}

// BatchPointsConfig is the config data needed to create an instance of the BatchPoints struct.
type BatchPointsConfig struct {
	// Precision is the write precision of the points, defaults to "ms".
	Precision string

	// Database is the database to write points to.
	Database string
}

// Client is a client interface for writing & querying the database.
type Client interface {
	// Ping checks that status of cluster
	Ping() (time.Duration, string, error)

	// Write takes a BatchPoints object and writes all Points to InfluxDB.
	Write(bp BatchPoints) error

	// Query makes an TDEngine Query on the database. This will fail if using
	// the UDP client.
	Query(q Query) (*Response, error)

	// Close releases any resources a Client may be using.
	Close() error
}

// NewHTTPClient returns a new Client from the provided config.
// Client is safe for concurrent use by multiple goroutines.
func NewHTTPClient(conf HTTPConfig) (Client, error) {
	if conf.UserAgent == "" {
		conf.UserAgent = "TDEngineDBClient"
	}

	u, err := url.Parse(conf.Addr)
	if err != nil {
		return nil, err
	} else if u.Scheme != "http" && u.Scheme != "https" {
		m := fmt.Sprintf("Unsupported protocol scheme: %s, your address"+
			" must start with http:// or https://", u.Scheme)
		return nil, errors.New(m)
	}

	switch conf.WriteEncoding {
	case DefaultEncoding, GzipEncoding:
	default:
		return nil, fmt.Errorf("unsupported encoding %s", conf.WriteEncoding)
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: conf.InsecureSkipVerify,
		},
		Proxy: conf.Proxy,
	}
	if conf.TLSConfig != nil {
		tr.TLSClientConfig = conf.TLSConfig
	}
	return &client{
		url:       *u,
		username:  conf.Username,
		password:  conf.Password,
		useragent: conf.UserAgent,
		httpClient: &http.Client{
			Timeout:   conf.Timeout,
			Transport: tr,
		},
		transport: tr,
		encoding:  conf.WriteEncoding,
	}, nil
}

// Ping will check to see if the server is up.
// Ping returns how long the request took, the version of the server it connected to, and an error if one occurred.
func (c *client) Ping() (time.Duration, string, error) {
	now := time.Now()
	var version string
	if resp, err := c.Query(NewQuery("select server_version() as version", "", "")); err != nil {
		return 0, "", err
	} else if resp != nil {
		version = resp.Data[resp.Rows-1][0].(string)
	} else {
		return 0, "", errors.New("get server version response empty")
	}
	return time.Since(now), version, nil
}

// Close releases the client's resources.
func (c *client) Close() error {
	c.transport.CloseIdleConnections()
	return nil
}

// client is safe for concurrent use as the fields are all read-only
// once the client is instantiated.
type client struct {
	// N.B - if url.UserInfo is accessed in future modifications to the
	// methods on client, you will need to synchronize access to url.
	url        url.URL
	username   string
	password   string
	useragent  string
	httpClient *http.Client
	transport  *http.Transport
	encoding   ContentEncoding
}

// BatchPoints is an interface into a batched grouping of points to write into
// InfluxDB together. BatchPoints is NOT thread-safe, you must create a separate
// batch for each goroutine.
type BatchPoints interface {
	// AddPoint adds the given point to the Batch of points.
	AddPoint(p *DataPoint)
	// AddPoints adds the given points to the Batch of points.
	AddPoints(ps []*DataPoint)
	// Points lists the points in the Batch.
	Points() []*DataPoint

	// Precision returns the currently set precision of this Batch.
	Precision() string
	// SetPrecision sets the precision of this batch.
	SetPrecision(s string) error

	// Database returns the currently set database of this Batch.
	Database() string
	// SetDatabase sets the database of this Batch.
	SetDatabase(s string)
}

// NewBatchPoints returns a BatchPoints interface based on the given config.
func NewBatchPoints(conf BatchPointsConfig) (BatchPoints, error) {
	if conf.Precision == "" {
		conf.Precision = "ms"
	}
	if _, err := time.ParseDuration("1" + conf.Precision); err != nil {
		return nil, err
	}
	bp := &batchpoints{
		database:  conf.Database,
		precision: conf.Precision,
	}
	return bp, nil
}

type batchpoints struct {
	points           []*DataPoint
	database         string
	precision        string
	retentionPolicy  string
	writeConsistency string
}

func (bp *batchpoints) AddPoint(p *DataPoint) {
	bp.points = append(bp.points, p)
}

func (bp *batchpoints) AddPoints(ps []*DataPoint) {
	bp.points = append(bp.points, ps...)
}

func (bp *batchpoints) Points() []*DataPoint {
	return bp.points
}

func (bp *batchpoints) Precision() string {
	return bp.precision
}

func (bp *batchpoints) Database() string {
	return bp.database
}

func (bp *batchpoints) WriteConsistency() string {
	return bp.writeConsistency
}

func (bp *batchpoints) RetentionPolicy() string {
	return bp.retentionPolicy
}

func (bp *batchpoints) SetPrecision(p string) error {
	if _, err := time.ParseDuration("1" + p); err != nil {
		return err
	}
	bp.precision = p
	return nil
}

func (bp *batchpoints) SetDatabase(db string) {
	bp.database = db
}

func (bp *batchpoints) SetWriteConsistency(wc string) {
	bp.writeConsistency = wc
}

func (bp *batchpoints) SetRetentionPolicy(rp string) {
	bp.retentionPolicy = rp
}

// DataPoint represents a single data point.
type DataPoint struct {
	pt Point
}

// NewDataPoint returns a point with the given timestamp. If a timestamp is not
// given, then data is sent to the database without a timestamp, in which case
// the server will assign local time upon reception. NOTE: it is recommended to
// send data with a timestamp.
func NewDataPoint(
	name string,
	tags map[string]string,
	fields map[string]interface{},
	t ...time.Time,
) (*DataPoint, error) {
	var T time.Time
	if len(t) > 0 {
		T = t[0]
	}

	pt, err := NewPoint(name, NewTags(tags), fields, T)
	if err != nil {
		return nil, err
	}
	return &DataPoint{
		pt: pt,
	}, nil
}

// String returns a line-protocol string of the Point.
func (p *DataPoint) String() string {
	return p.pt.String()
}

// PrecisionString returns a line-protocol string of the Point,
// with the timestamp formatted for the given precision.
func (p *DataPoint) PrecisionString(precision string) string {
	return p.pt.PrecisionString(precision)
}

// Name returns the measurement name of the point.
func (p *DataPoint) Name() string {
	return string(p.pt.Name())
}

// Tags returns the tags associated with the point.
func (p *DataPoint) Tags() map[string]string {
	return p.pt.Tags().Map()
}

// Time return the timestamp for the point.
func (p *DataPoint) Time() time.Time {
	return p.pt.Time()
}

// UnixNano returns timestamp of the point in nanoseconds since Unix epoch.
func (p *DataPoint) UnixNano() int64 {
	return p.pt.UnixNano()
}

// Fields returns the fields for the point.
func (p *DataPoint) Fields() (map[string]interface{}, error) {
	return p.pt.Fields()
}

// NewPointFrom returns a point from the provided models.Point.
func NewPointFrom(pt Point) *DataPoint {
	return &DataPoint{pt: pt}
}

func (c *client) Write(bp BatchPoints) error {
	var b bytes.Buffer

	var w io.Writer
	if c.encoding == GzipEncoding {
		w = gzip.NewWriter(&b)
	} else {
		w = &b
	}

	for _, p := range bp.Points() {
		if p == nil {
			continue
		}
		if _, err := io.WriteString(w, p.pt.PrecisionString(bp.Precision())); err != nil {
			return err
		}

		if _, err := w.Write([]byte{'\n'}); err != nil {
			return err
		}
	}

	// gzip writer should be closed to flush data into underlying buffer
	if c, ok := w.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return err
		}
	}

	u := c.url
	u.Path = path.Join(u.Path, WriteDataURL)

	req, err := http.NewRequest("POST", u.String(), &b)
	if err != nil {
		return err
	}
	if c.encoding != DefaultEncoding {
		req.Header.Set("Content-Encoding", string(c.encoding))
	}
	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", c.useragent)
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	params := req.URL.Query()
	params.Set("db", bp.Database())
	params.Set("precision", bp.Precision())
	req.URL.RawQuery = params.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		var err = errors.New(string(body))
		return err
	}

	return nil
}

// Query defines a query to send to the server.
type Query struct {
	Command   string
	Database  string
	Precision string
}

// NewQuery returns a query object.
// The database and precision arguments can be empty strings if they are not needed for the query.
func NewQuery(command, database, precision string) Query {
	return Query{
		Command:   command,
		Database:  database,
		Precision: precision,
	}
}

var ErrNotExistsTable = errors.New("table does not exist")

// Response represents a list of statement results.
type Response struct {
	Code       int             `json:"code,omitempty"`
	Desc       string          `json:"desc,omitempty"`
	ColumnMeta [][]interface{} `json:"column_meta,omitempty"`
	Data       [][]interface{} `json:"data,omitempty"`
	Rows       int             `json:"rows,omitempty"`
}

// Error returns the first error from any statement.
func (r *Response) Error() error {
	if r.Code != 0 || len(r.Desc) > 0 {
		//if strings.Contains(r.Desc, "error: Table does not exist") || strings.Contains(r.Desc, "Database not specified") {
		//	return ErrNotExistsTable
		//}
		if r.Code == 9826 || r.Code == 9750 {
			return ErrNotExistsTable
		}
		return errors.New(r.Desc)
	}
	return nil
}

// Query sends a command to the server and returns the Response.
func (c *client) Query(q Query) (*Response, error) {
	req, err := c.createDefaultRequest(q)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	var response Response
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	decErr := dec.Decode(&response)

	// ignore this error if we got an invalid status code
	if decErr != nil && decErr.Error() == "EOF" && resp.StatusCode != http.StatusOK {
		decErr = nil
	}
	// If we got a valid decode error, send that back
	if decErr != nil {
		return nil, fmt.Errorf("unable to decode json: received status code %d err: %s", resp.StatusCode, decErr)
	}

	// If we don't have an error in our json response, and didn't get statusOK
	// then send back an error
	if resp.StatusCode != http.StatusOK && response.Error() == nil {
		return &response, fmt.Errorf("received status code %d from server", resp.StatusCode)
	}
	return &response, nil
}

func checkResponse(resp *http.Response) error {
	if resp.StatusCode >= http.StatusInternalServerError {
		body, err := io.ReadAll(resp.Body)
		if err != nil || len(body) == 0 {
			return fmt.Errorf("received status code %d from downstream server", resp.StatusCode)
		}

		return fmt.Errorf("received status code %d from downstream server, with response body: %q", resp.StatusCode, body)
	}

	// If we get an unexpected content type, then it is also not from influx direct and therefore
	// we want to know what we received and what status code was returned for debugging purposes.
	if cType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); cType != "application/json" {
		// Read up to 1kb of the body to help identify downstream errors and limit the impact of things
		// like downstream serving a large file
		body, err := ioutil.ReadAll(io.LimitReader(resp.Body, 1024))
		if err != nil || len(body) == 0 {
			return fmt.Errorf("expected json response, got empty body, with status: %v", resp.StatusCode)
		}

		return fmt.Errorf("expected json response, got %q, with status: %v and response body: %q", cType, resp.StatusCode, body)
	}
	return nil
}

func (c *client) createDefaultRequest(q Query) (*http.Request, error) {
	u := c.url
	u.Path = path.Join(u.Path, ExecuteSqlURL)
	if len(q.Database) > 0 {
		u.Path = path.Join(u.Path, q.Database)
	}

	req, err := http.NewRequest("POST", u.String(), bytes.NewBufferString(q.Command))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", c.useragent)

	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	return req, nil

}
