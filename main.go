package main

import (
	"crypto/sha256"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1"

	"golang.org/x/net/context"

	"github.com/pborman/uuid"
)

func createClients(ctx context.Context, db string) (*database.DatabaseAdminClient, *spanner.Client) {
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Fatal(fmt.Sprintf("database.NewDatabaseAdminClient err. %s", err.Error()))
	}

	dataClient, err := spanner.NewClient(ctx, db)
	if err != nil {
		log.Fatal(fmt.Sprintf("spanner.NewClient err. %s", err.Error()))
	}

	return adminClient, dataClient
}

func run(ctx context.Context, client *spanner.Client, filePath string) {
	fp, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer fp.Close()

	reader := csv.NewReader(fp)
	reader.Comma = ','
	_, err = reader.Read()
	if err != nil {
		fmt.Println("header skip mis!")
	}

	var rows [][]string
	count := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		rows = append(rows, record)
		count++
		if count >= 1000 {
			err = write(ctx, client, rows)
			if err != nil {
				fmt.Printf("Spanner write err. %s\n", err.Error())
			} else {
				fmt.Println("Done! 1000 Rows!")
			}
			count = 0
			rows = [][]string{}
		}
	}
}

func write(ctx context.Context, client *spanner.Client, rows [][]string) error {
	columns := []string{"ID", "Keyword", "Keyword_ID", "Utterance"}

	s := sha256.New()
	var m []*spanner.Mutation
	for _, row := range rows {
		keywordId, err := strconv.ParseInt(row[1], 0, 0)
		if err != nil {
			fmt.Printf("KeyWordID Parse Error. v = %s\n", row[1])
			continue
		}

		io.WriteString(s, uuid.New())
		key := fmt.Sprintf("%x", s.Sum(nil))
		m = append(m, spanner.InsertOrUpdate("Utterance", columns, []interface{}{key, row[0], keywordId, row[2]}))
	}

	_, err := client.Apply(ctx, m)
	return err
}

func main() {
	flag.Parse()
	if len(flag.Args()) != 3 {
		flag.Usage()
		os.Exit(2)
	}

	db, filePath := flag.Arg(0), flag.Arg(1)
	end, err := strconv.ParseInt(flag.Arg(2), 0, 0)
	if err != nil {
		fmt.Println("end is not int")
		os.Exit(2)
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < int(end); i++ {
		wg.Add(1)
		fp := fmt.Sprintf("%s/utterance-%012d.csv", filePath, i)
		go func() {
			ctx, _ := context.WithTimeout(context.Background(), 6*time.Hour)
			_, dataClient := createClients(ctx, db)

			run(ctx, dataClient, fp)
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println("finish !")
}
