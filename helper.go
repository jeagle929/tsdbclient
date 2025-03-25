package tsdbclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	tsdbTimeStampFormat = "2006-01-02T15:04:05.999999999Z"
	taosPollTimeoutMs   = 5000
)

var (
	once          sync.Once
	clientWrapper TSDBClient
)

type TSDBClient interface {
	GetHttpClient() Client

	QueryData(string, bool) ([]map[string]interface{}, error)
	WriteData(int64, string, map[string]string, map[string]interface{}) error
	Close() error

	Subscribe(ctx context.Context, topic string, chMessage chan<- TSDBSubscribedMessage) error
	UnSubscribe(topic string) error
}

type tsdbClient struct {
	httpClient Client
	dbConfig   struct {
		DBAddr    string
		DBName    string
		Precision string
		DBUser    string
		DBPass    string
	}
	initialErr error

	//consumers map[string]TSDBSubscribeConsumer
	//lockRW    sync.RWMutex

	defaultNumberValue interface{}
}

func NewTDEngineClient(opts ...DBOption) TSDBClient {
	dbOpt := newDBOptions(opts...)
	config := HTTPConfig{
		Addr:     dbOpt.DatabaseAddr,
		Username: dbOpt.DatabaseUser,
		Password: dbOpt.DatabasePass,
	}

	cli := &tsdbClient{
		//consumers:          make(map[string]TSDBSubscribeConsumer),
		defaultNumberValue: dbOpt.DefaultNumberValue,
	}
	cli.httpClient, cli.initialErr = NewHTTPClient(config)
	cli.dbConfig.DBAddr = dbOpt.DatabaseAddr
	cli.dbConfig.DBName = dbOpt.DatabaseName
	cli.dbConfig.Precision = dbOpt.PrecisionUnit
	cli.dbConfig.DBUser = dbOpt.DatabaseUser
	cli.dbConfig.DBPass = dbOpt.DatabasePass

	return cli
}

func (client *tsdbClient) GetHttpClient() Client {
	return client.httpClient
}

func (client *tsdbClient) QueryData(sql string, convertNumber bool) (result []map[string]interface{}, err error) {

	if client.httpClient == nil || client.initialErr != nil {
		err = fmt.Errorf("not created http client for tdengine: %v", client.initialErr)
		return
	}

	var resp *Response
	resp, err = client.httpClient.Query(NewQuery(sql, client.dbConfig.DBName, client.dbConfig.Precision))
	if err == nil {
		if err = resp.Error(); err != nil {
			if err == ErrNotExistsTable {
				return result, nil
			}
			return nil, err
		}
		for _, r := range resp.Data {
			row := map[string]interface{}{}
			for i, c := range resp.ColumnMeta {
				// c is column meta, format: [column name, column type, type size]
				if len(c) != 3 {
					return nil, errors.New("column meta data length no equal 3")
				}
				cn := c[0].(string)
				// if column name is `_`, ignore
				if cn == "_" {
					continue
				}
				if convertNumber {
					switch c[1].(string) {
					case "BIGINT", "INT", "TINYINT", "SMALLINT", "TINYINT UNSIGNED", "SMALLINT UNSIGNED", "INT UNSIGNED", "BIGINT UNSIGNED":
						if num, ok := r[i].(json.Number); ok {
							row[cn], _ = num.Int64()
						} else {
							row[cn] = client.defaultNumberValue
						}
						//row[cn], _ = r[i].(json.Number).Int64()
					case "FLOAT", "DOUBLE":
						if num, ok := r[i].(json.Number); ok {
							row[cn], _ = num.Float64()
						} else {
							row[cn] = client.defaultNumberValue
						}
						//row[cn], _ = r[i].(json.Number).Float64()
					case "TIMESTAMP":
						if ts, ee := time.Parse(tsdbTimeStampFormat, r[i].(string)); ee == nil {
							row[cn] = ts.Unix()
						} else {
							row[cn] = 0
						}
					default:
						row[cn] = r[i]
					}
				} else {
					row[cn] = r[i]
				}
			}
			result = append(result, row)
		}
	}

	return
}

func (client *tsdbClient) WriteData(ts int64, name string, tags map[string]string, fields map[string]interface{}) error {

	bps, _ := NewBatchPoints(BatchPointsConfig{
		Precision: client.dbConfig.Precision,
		Database:  client.dbConfig.DBName,
	})

	if ts > 0 {
		var t time.Time
		switch client.dbConfig.Precision {
		case "s":
			t = time.Unix(ts, 0)
		case "us":
			t = time.UnixMicro(ts)
		case "ns":
			t = time.Unix(0, ts)
		default: // ms
			t = time.UnixMilli(ts)
		}
		if pt, err := NewDataPoint(name, tags, fields, t); err != nil {
			return err
		} else {
			bps.AddPoint(pt)
		}
	} else {
		if pt, err := NewDataPoint(name, tags, fields); err != nil {
			return err
		} else {
			bps.AddPoint(pt)
		}
	}

	return client.httpClient.Write(bps)

}

func (client *tsdbClient) Subscribe(ctx context.Context, topic string, chMessage chan<- TSDBSubscribedMessage) error {
	return client.subscribe(ctx, topic, chMessage)
}

func (client *tsdbClient) subscribe(ctx context.Context, topic string, chMessage chan<- TSDBSubscribedMessage) error {

	if len(topic) == 0 {
		return errors.New("invalid args: topic is empty")
	}

	if chMessage == nil {
		return errors.New("invalid args: chMessage is nil")
	}

	tsdbCons, err := newConsumer(client.dbConfig.DBAddr, client.dbConfig.DBUser, client.dbConfig.DBPass, topic)
	if err != nil {
		return err
	}
	defer tsdbCons.Close()

	err = tsdbCons.Subscribe(topic, nil)
	if err != nil {
		return err
	}
	defer tsdbCons.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			log.Println("[tsdbclient] Subscribe timeout to ready unsubscribe...")
			if e := tsdbCons.Unsubscribe(); e != nil {
				log.Printf("[tsdbclient] Subscribe unsubscribe error: %v\n", e)
				return e
			} else {
				log.Println("[tsdbclient] Subscribe unsubscribe success.")
				close(chMessage)
				log.Println("[tsdbclient] Subscribe receive channel closed")
			}
			return nil
		default:
			if ev := tsdbCons.Poll(taosPollTimeoutMs); ev != nil {
				switch e := ev.(type) {
				case TSDBSubscribedMessage:
					select {
					case chMessage <- e:
					default:
						log.Println("[tsdbclient] Subscribe chan message full")
					}
				case error:
					log.Printf("[tsdbclient] Subscribe tmq error: %v\n", e)
					//close(chMessage)
					return e
				default:
					log.Printf("[tsdbclient] Subscribe not expected receive type: %T\n", e)
				}
			}
		}
	}

}

// Deprecated: replace with subscribe args ctx
func (client *tsdbClient) UnSubscribe(topic string) error {
	//if len(topic) == 0 {
	//	return errors.New("invalid args: topic is empty")
	//}
	//
	//client.lockRW.Lock()
	//defer client.lockRW.Unlock()
	//
	//if cs, ok := client.consumers[topic]; ok {
	//	cs.Unsubscribe()
	//	cs.Close()
	//	delete(client.consumers, topic)
	//}
	return nil
}

// Close releases any resources a Client may be using.
func (client *tsdbClient) Close() error {
	//client.lockRW.Lock()
	//defer client.lockRW.Unlock()
	//for _, v := range client.consumers {
	//	v.Unsubscribe()
	//	v.Close()
	//}
	//clear(client.consumers)
	return client.httpClient.Close()
}

func init() {
	once.Do(func() {
		if clientWrapper == nil {
			clientWrapper = NewTDEngineClient()
		}
	})
}

///////////////////////////////////////////////////////////////////////////////////////////////

func ReadData(sql string, opts ...DBOption) ([]map[string]interface{}, error) {
	dbOpt := newDBOptions(opts...)
	return clientWrapper.QueryData(sql, dbOpt.ConvertNumber)
}

func WriteData(name string, tag map[string]string, fields map[string]interface{}, opts ...DBOption) error {
	dbOpt := newDBOptions(opts...)
	return clientWrapper.WriteData(dbOpt.Timestamp, name, tag, fields)
}

func QueryData(sql string, opts ...DBOption) (columns []string, rows [][]interface{}, err error) {
	if client := clientWrapper.GetHttpClient(); client != nil {
		dbOpt := newDBOptions(opts...)
		if resp, e := client.Query(NewQuery(sql, dbOpt.DatabaseName, dbOpt.PrecisionUnit)); e == nil {
			for _, cm := range resp.ColumnMeta {
				columns = append(columns, cm[0].(string))
			}
			rows = resp.Data
		} else {
			err = e
		}
	} else {
		err = errors.New("default http client is nil")
	}

	return

}

func QueryCount(field, tableName, filter string) (count int64, err error) {
	sql := fmt.Sprintf("select count(`%s`) as `count` from `%s` ", field, tableName)
	if len(filter) > 0 {
		if strings.HasPrefix(filter, "where") {
			sql += filter
		} else {
			sql += fmt.Sprintf("where %s", filter)
		}
	}
	sql += ";"

	if resp, e := clientWrapper.QueryData(sql, false); e != nil {
		err = e
	} else if resp != nil && len(resp) > 0 {
		if v, ok := resp[0]["count"]; ok {
			count, err = v.(json.Number).Int64()
		} else {
			count = -1
			err = errors.New("not result field: count")
		}
	}

	return
}

func TableIfExists(tableName string, super bool) bool {
	if len(tableName) > 0 {
		sql := fmt.Sprintf("show tables like '%s';", tableName)
		if super {
			sql = fmt.Sprintf("show stables like '%s';", tableName)
		}
		if rows, e := clientWrapper.QueryData(sql, false); e == nil && len(rows) > 0 {
			return true
		}
	}
	return false
}

func GetDatabaseName() string {
	dbOpt := newDBOptions()
	return dbOpt.DatabaseName
}
