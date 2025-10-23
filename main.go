package main

import (
	"context"
	"flag"
	"log"
	"os"
	tgClient "read-adviser-bot/clients/telegram"
	event_consumer "read-adviser-bot/consumer/event-consumer"
	"read-adviser-bot/events/telegram"
	"read-adviser-bot/storage/postgresql"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

const (
	tgBotHost   = "api.telegram.org"
	storagePath = "files_storage"
	batchSize   = 100
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("can't load .env file")
	}
}

func main() {
	//s := files.New(storagePath)
	s, err := postgresql.New(
		"host=" + os.Getenv("POSTGRES_HOST") +
			" port=" + os.Getenv("POSTGRES_PORT") +
			" user=" + os.Getenv("POSTGRES_USER") +
			" password=" + os.Getenv("POSTGRES_PASSWORD") +
			" dbname=" + os.Getenv("POSTGRES_DBNAME") +
			" sslmode=disable",
	)
	if err != nil {
		log.Fatalf("can't connect to the database: %v", err)
	}

	if err := s.Init(context.TODO()); err != nil {
		log.Fatalf("can't initialize database: %v", err)
	}

	eventsProcessor := telegram.New(
		tgClient.New(tgBotHost, mustToken()),
		s,
	)

	log.Print("service started")

	consumer := event_consumer.New(eventsProcessor, eventsProcessor, batchSize)
	if err := consumer.Start(context.TODO()); err != nil {
		log.Fatal("service is stopped", err)
	}
}

func mustToken() string {
	token := flag.String(
		"tg-bot-token",
		"",
		"token for access to telegram bot",
	)

	flag.Parse()

	if *token == "" {
		log.Fatal("token is not specified")
	}

	return *token
}
