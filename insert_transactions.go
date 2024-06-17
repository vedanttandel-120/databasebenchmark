package main

import (
	"context"
	"fmt"
	"time"
)

func InsertTransaction(ctx context.Context) {
	var tran TransactionHistoryMDB
	tran.Amount = "123.00"
	tran.TransactionType = 2
	tran.TerminalID = "1000"
	tran.AudioPlayed = 0
	tran.DeviceID = 1000
	tran.RRN = "fffsf"
	tran.RequestRefNo = "sdfsdsg"
	avg := 0.0
	totalStart := time.Now()
	tran.TimeSeriesStamp=time.Now().UTC()
	for x := 0; x < 100000; x++ {
		if x%10000 == 0 {
			tran.DeviceID += 1
			tran.TimeSeriesStamp = time.Now().UTC()
			fmt.Println("Average Insert Time", avg)
			
		}
		tran.TimeSeriesStamp= tran.TimeSeriesStamp.Add(-3 * time.Second)
		tran.TMsgRecvByServer = time.Now().UTC().Unix() - int64(x%10000*3)
		start := time.Now()
		// status := InsertTransactionHistoryMDBQ(ctx, tran, "transactionSeries")
		status := InsertTransactionHistoryMDBQ(ctx, tran, "transactions")
		if !status {
			fmt.Errorf("Unable to insert Transaction to DB")
			break
		}
		timeElapsed := time.Since(start).Seconds()
		fmt.Println(timeElapsed)
		avg += timeElapsed / float64(x+1)
		// logs.WriteLog.Debug("Time For Insertion", zap.Any("Time Taken", timeElapsed))
	}
	totalTime := time.Since(totalStart).Seconds()
	fmt.Println("Average Insert Time", avg)
	fmt.Println("Total Insert Time", totalTime)
}
