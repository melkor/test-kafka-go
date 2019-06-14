package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/davecgh/go-spew/spew"
	"github.com/spf13/pflag"
)

var (
	topic = pflag.StringP("topic", "t", "melkor_talk_to_adnan", "ze topik")
)

func main() {
	pflag.Parse()

	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Consumer.Return.Errors = true

	kafkaClient, err := sarama.NewClient([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		err := kafkaClient.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}()

	spew.Dump(kafkaClient.Partitions(*topic))
	// Start a new consumer group
	group, err := sarama.NewConsumerGroupFromClient("id-of-consumers", kafkaClient)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := group.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}()

	// Track errors
	go func() {
		for err := range group.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	// Iterate over consumer sessions.
	ctx := context.Background()
	for {
		topics := []string{*topic}
		handler := exampleConsumerGroupHandler{}

		err := group.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}
