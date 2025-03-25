package tsdbclient

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"

	tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
	"github.com/taosdata/driver-go/v3/ws/tmq"
)

type TopicMode int8

const (
	_ TopicMode = iota
	DBMode
	STMode
	SQLMode
)

type TSDBSubscribedMessage interface {
	Topic() string
	DBName() string
	Value() interface{}
	Offset() tmqcommon.Offset
}

type taosConsumer interface {
	Subscribe(topic string, rebalanceCb tmq.RebalanceCb) error
	Poll(timeoutMs int) tmqcommon.Event
	Unsubscribe() error
	Close() error
}

func newConsumer(dbAddr, dbUser, dbPass, topic string) (consumer taosConsumer, err error) {

	hn, _ := os.Hostname()

	consumer, err = tmq.NewConsumer(&tmqcommon.ConfigMap{
		"ws.url":             fmt.Sprintf("%s/rest/tmq", strings.ReplaceAll(dbAddr, "http:", "ws:")),
		"td.connect.user":    dbUser,
		"td.connect.pass":    dbPass,
		"group.id":           topic,
		"client.id":          fmt.Sprintf("iot_%s-%d", hn, rand.Intn(86400)),
		"auto.offset.reset":  "latest",
		"enable.auto.commit": "true",
		//"auto.commit.interval.ms": "5000",
	})

	return
}

func Subscribe(ctx context.Context, topic string, chMessage chan<- TSDBSubscribedMessage, chError chan<- error) error {
	go func() {
		chError <- clientWrapper.Subscribe(ctx, topic, chMessage)
	}()
	return nil
}

func CreateTopic(topic, content string, mode TopicMode) error {

	if len(topic) == 0 || len(content) == 0 {
		return errors.New("miss args: `topic` or `content`")
	}

	var sql string

	switch mode {
	case DBMode: // database
		sql = fmt.Sprintf("create topic if not exists %s as database %s", topic, content)
	case STMode: // stable
		sql = fmt.Sprintf("create topic if not exists %s as stable %s", topic, content)
	case SQLMode: // sql
		sql = fmt.Sprintf("create topic if not exists %s as %s", topic, content)
	default:
		return fmt.Errorf("not support mode: %d", mode)
	}

	_, err := ReadData(sql)

	return err
}

func DropTopic(topic string) error {

	if len(topic) == 0 {
		return fmt.Errorf("invalid args: `topic` is empty")
	}

	_, err := ReadData(fmt.Sprintf("drop topic if exists %s", topic))

	return err
}
