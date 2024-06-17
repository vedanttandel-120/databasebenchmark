package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func FetchTransactionSeries(ctx context.Context) {
	fmt.Println("fetching Transaction")


	avg := 0.0
	totalStart := time.Now()
	for x := 1001; x < 1011; x++ {
		start := time.Now()
		status := FetchTransactionseriesMG(ctx, x)
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

func FetchTransactionseriesMG(ctx context.Context, x int) (bool) {
	end := time.Now()
	start := time.Date(end.Year(), end.Month()-1, 1, 0, 0, 0, 0, time.UTC)
	// Options for sorting and pagination
	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"timeSeriesStamp", 1}}) // Sorting by timestamp ascending
	filter := bson.M{
		
		"timeSeriesStamp": bson.M{
			"$gte": start,
			"$lt":  end,
		},
		"deviceId": x,
	}
	cursor, err := MongoDatabase.Collection("transactionSeries").Find(ctx, filter, findOptions)
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
