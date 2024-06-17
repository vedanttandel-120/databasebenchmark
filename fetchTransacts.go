package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func FetchTransactions(ctx context.Context) {
	fmt.Println("fetching Transaction")


	avg := 0.0
	totalStart := time.Now()
	for x := 1001; x < 1011; x++ {
		start := time.Now()
		status := FetchTransactionsMG(ctx, x)
		// status := InsertTransactionHistoryMDBQ(ctx, tran, "transactions")
		if !status {
			fmt.Errorf("Unable to Fetch Transaction to DB")
			break
		}
		timeElapsed := time.Since(start).Seconds()
		fmt.Println(timeElapsed)
		avg += timeElapsed / float64(x+1)
		// logs.WriteLog.Debug("Time For Insertion", zap.Any("Time Taken", timeElapsed))
	}
	totalTime := time.Since(totalStart).Seconds()
	fmt.Println("Average Fetch Time", avg)
	fmt.Println("Total Fetch Time", totalTime)

	// Perform the query
	
}

func FetchTransactionsMG(ctx context.Context, x int) (bool) {
	end := time.Now().UTC().Unix()
	start := end - 2592000
	// pageSize := 100
	// pageNumber := 1
	// skip := (pageNumber - 1) * pageSize

	// Options for sorting and pagination
	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"tMsgRecvByServer", 1}}) // Sorting by timestamp ascending
	// findOptions.SetSkip(int64(skip))
	// findOptions.SetLimit(int64(pageSize))
	filter := bson.M{
		"tMsgRecvByServer": bson.M{
			"$gte": start,
			"$lt":  end,
		},
		"deviceId": x,
	}
	cursor, err := MongoDatabase.Collection("transactions").Find(ctx, filter, findOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(ctx)

	// Iterate over the cursor and process each document
	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			log.Fatal(err)
		}
		// fmt.Println(result)
	}

	// Check for errors during cursor iteration
	if err := cursor.Err(); err != nil {
		log.Fatal(err)
		return false
	}
	return true
}
