package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"os"
	"os/signal"
	"time"
)

type KafkaConfig struct {
	User      string
	Passwd    string
	BrokerURL string
	GroupID   string
	Topic     string
	StartTime string
}

func main() {
	var config KafkaConfig
	config.User = ""
	config.Passwd = ""
	config.BrokerURL = ""
	config.GroupID = ""
	config.Topic = ""
	config.StartTime = ""

	clusterConsumer(config)
}

func clusterConsumer(con KafkaConfig) {
	config := cluster.NewConfig()
	config.Version = sarama.V2_3_0_0
	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = "PLAIN"
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Net.MaxOpenRequests = 100
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Net.SASL.User = con.User
	config.Net.SASL.Password = con.Passwd

	consumer, err := cluster.NewConsumer([]string{con.BrokerURL}, con.GroupID, []string{con.Topic}, config)
	if err != nil {
		return
	}
	defer consumer.Close()

	var startTime int64
	if con.StartTime == "" {
		startTime = 0
	} else {
		formatTime, err := time.ParseInLocation("2006-01-02 15:04:05", con.StartTime, time.Local)
		if err != nil {
			fmt.Println(err)
			return
		}
		startTime = formatTime.Unix()
	}

	// trap SIGINT to trigger a shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			panic(err)
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			fmt.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	// consume messages, watch signals
	var successes int
Loop:
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				if msg.Timestamp.Unix() < startTime {
					continue
				}
				table, operation, sql, err := parseData(msg.Value)
				if err != nil {
					fmt.Println("Create consumer failed,", err)
					return
				}
				fmt.Fprintf(os.Stdout, "%s:%s/%d/%d\tdb:%s\ttable:%s\toperation:%s\tsql:%s\n",
					"group_id", "topic",
					msg.Partition, msg.Offset, msg.Key, table, operation, sql)

				consumer.MarkOffset(msg, "")
				successes++
			}

		case <-signals:
			break Loop
		}
	}
	fmt.Fprintf(os.Stdout, "%s consume %d messages \n", con.GroupID, successes)
}

func parseData(data []byte) (string, string, string, error) {
	type SubData struct {
		TableName string `json:"table"`
		Operation string `json:"operation"`
		Sql       string `json:"sql"`
	}

	d := SubData{}
	err := json.Unmarshal(data, &d)
	if err != nil {
		return "", "", "", fmt.Errorf("Data format is wrong.")
	}

	return d.TableName, d.Operation, d.Sql, nil
}
