package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/spf13/pflag"
)

var (
	message = pflag.StringP("message", "p", "lol ta rien mi", "ze message")
	topic   = pflag.StringP("topic", "t", "melkor_talk_to_adnan", "ze topik")
)

func main() {
	pflag.Parse()
	fmt.Println("coucou adnan")
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	msg := &sarama.ProducerMessage{Topic: *topic, Value: sarama.StringEncoder(*message)}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("FAILED to send message: %s\n", err)
	} else {
		log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
	}

}
