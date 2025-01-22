package tsdbclient

import (
	"fmt"
	"os"
)

type DbOptions struct {
	DatabaseAddr  string
	DatabaseName  string
	PrecisionUnit string
	DatabaseUser  string
	DatabasePass  string

	ConvertNumber bool
	Timestamp     int64
}

type DBOption func(*DbOptions)

func DatabaseAddr(u string) DBOption {
	return func(dbOpts *DbOptions) {
		dbOpts.DatabaseAddr = u
	}
}

func DatabaseName(d string) DBOption {
	return func(dbOpts *DbOptions) {
		dbOpts.DatabaseName = d
	}
}

func PrecisionUnit(p string) DBOption {
	return func(dbOpts *DbOptions) {
		dbOpts.PrecisionUnit = p
	}
}

func DatabaseUser(u string) DBOption {
	return func(dbOpts *DbOptions) {
		dbOpts.DatabaseUser = u
	}
}

func DatabasePass(p string) DBOption {
	return func(dbOpts *DbOptions) {
		dbOpts.DatabasePass = p
	}
}

func ConvertNumber(c bool) DBOption {
	return func(dbOpts *DbOptions) {
		dbOpts.ConvertNumber = c
	}
}

func Timestamp(ts int64) DBOption {
	return func(dbOpts *DbOptions) {
		dbOpts.Timestamp = ts
	}
}

const (
	envDBHost = "SVC_IOT_TDENGINE_HOST"
	envDBPort = "SVC_IOT_TDENGINE_PORT"
	envDBUser = "SVC_IOT_TDENGINE_USER"
	envDBPass = "SVC_IOT_TDENGINE_PASS"
	envDBPrec = "SVC_IOT_TDENGINE_PREC"
	envDBName = "SVC_IOT_TDENGINE_DB"
)

func newDBOptions(options ...DBOption) DbOptions {
	var opts []DBOption

	if v1 := os.Getenv(envDBHost); len(v1) > 0 {
		if v2 := os.Getenv(envDBPort); len(v2) > 0 {
			opts = append(opts, DatabaseAddr(fmt.Sprintf("http://%s:%s", v1, v2)))
		} else {
			opts = append(opts, DatabaseAddr(fmt.Sprintf("http://%s:6041", v1)))
		}
	}

	if v := os.Getenv(envDBUser); len(v) > 0 {
		opts = append(opts, DatabaseUser(v))
	}

	if v := os.Getenv(envDBPass); len(v) > 0 {
		opts = append(opts, DatabasePass(v))
	}

	if v := os.Getenv(envDBPrec); len(v) > 0 {
		opts = append(opts, PrecisionUnit(v))
	}

	if v := os.Getenv(envDBName); len(v) > 0 {
		opts = append(opts, DatabaseName(v))
	}

	if len(options) > 0 {
		opts = append(opts, options...)
	}

	opt := DbOptions{
		DatabaseAddr:  "http://127.0.0.1:6041",
		DatabaseName:  "iot",
		DatabaseUser:  "root",
		DatabasePass:  "taosdata",
		PrecisionUnit: "ms",
	}
	for _, o := range opts {
		o(&opt)
	}
	return opt
}
