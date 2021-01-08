package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// the topic and broker address are initialized as constant
const (
	topic          = "my-kafka-topic"
	broker1Address = "localhost:9093"
	broker2Address = "localhost:9094"
	broker3Address = "localhost:9095"
)

func produce(ctx context.Context) {
	// initialize the counter
	i := 0

	// initialize the writer with the broker addresses ,and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker1Address, broker2Address, broker3Address},
		Topic:   topic,
	})

	for {
		// each kafka message has the key and value. the key used
		// to decide which partitions (and consequently ,which broker)
		// the message get published on

		err := w.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(i)),
			// create the aribitory message payload for the value
			Value: []byte("this is message " + strconv.Itoa(i)),
		})

		if err != nil {
			panic("could not write the  message " + err.Error())
		}

		// log the confirmation once the message is written
		fmt.Println("writes:", i)
		i++
		// sleep for the seconds
		time.Sleep(time.Second)
	}
}

func consume(ctx context.Context) {
	// initalize a new reader with the brokers and topic
	// the groupID idntifies the consumer and prevents
	// it from receiving the messages

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address, broker2Address, broker3Address},
		Topic:   topic,
		GroupID: "group2",
	})
	for {
		// the 'ReadMessage' methods block until we receive the next event
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read the message:" + err.Error())
		}

		// after the receiving the message , log its value
		fmt.Println("recevied ", string(msg.Value))
	}
}

func main() {
	// create the new context
	ctx := context.Background()
	// produce messsage in  new go routine,since
	// both the produce and consume functions are
	// blocking

	go produce(ctx)
	consume(ctx)
}
