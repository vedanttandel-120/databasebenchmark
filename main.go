package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var MongoDatabase *mongo.Database

func main() {
	ctx := context.Background()
	uri := "mongodb://localhost:27017/?replicaSet=myReplicaSet"

	client, err := mongo.Connect(context.TODO(), options.Client().
		ApplyURI(uri))
	if err != nil {
		panic(err)
	}

	// set database and get mongo database client
	MongoDatabase = client.Database("mongo-sbdb")

	// CreateTransactionTimeSeriesMDBQ(ctx)
	// CreateTransactionMDBQ(ctx)

	defer func() {
		fmt.Println("disconnecting")
		if err := client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	// InsertTransaction(ctx)
	// FetchTransactions(ctx)
	FetchTransactionSeries(ctx)


	fmt.Println("done")
}

func InsertTransactionHistoryMDBQ(ctx context.Context, transactionInfo TransactionHistoryMDB, collectionName string) bool {
	_, err := MongoDatabase.Collection(collectionName).InsertOne(ctx, transactionInfo)
	if err != nil {
		fmt.Println("Failed to insert transactionInfo data to the database", err.Error())
		return false
	}
	return true
}

// helper func to set indexing to the collection
func CreateTransactionTimeSeriesMDBQ(ctx context.Context) bool {
	fmt.Println("Entering func: CreateTransactionTimeSeriesMDBQ")

	tso := options.TimeSeries().SetTimeField("timeSeriesStamp").SetMetaField("deviceId")
	opts := options.CreateCollection().SetTimeSeriesOptions(tso)
	err := MongoDatabase.CreateCollection(context.TODO(), "transactionSeries", opts)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	fmt.Println("Exiting func: CreateTransactionHistoryIndexMDBQ")

	// index for setting unique constraint
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{{"deviceId", 1}, {"audioPlayed", 1}},
		},
		{
			Keys: bson.D{{"tMsgRecvByServer", 1}, {"deviceId", 1}},
		},
		{
			Keys: bson.D{{"reqRefNo", 1}},
		},
	}

	// Setting unique constraint to the deviceId field
	_, err = MongoDatabase.Collection("transactionSeries").Indexes().CreateMany(ctx, indexes)
	if err != nil {
		fmt.Println(err.Error)
		return false
	}
	return true
}

// helper func to set indexing to the collection
func CreateTransactionMDBQ(ctx context.Context) bool {
	fmt.Println("Entering func: CreateTransactionMDBQ")

	// index for setting unique constraint
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{{"deviceId", 1}, {"audioPlayed", 1}},
		},
		{
			Keys: bson.D{{"tMsgRecvByServer", 1}, {"deviceId", 1}},
		},
		{
			Keys: bson.D{{"reqRefNo", 1}},
		},
	}

	// Setting unique constraint to the deviceId field
	_, err := MongoDatabase.Collection("transactions").Indexes().CreateMany(ctx, indexes)
	if err != nil {
		fmt.Println(err.Error)
		return false
	}
	return true
}

type TransactionHistoryMDB struct {
	ID                   primitive.ObjectID `bson:"_id,omitempty"`
	TimeSeriesStamp      time.Time          `bson:"timeSeriesStamp,omitempty" json:"timeSeriesStamp"`
	RequestRefNo         string             `bson:"reqRefNo" json:"reqRefNo" binding:"required"`
	DeviceID             int64              `bson:"deviceId" json:"deviceId"`
	TMsgRecvByServer     int64              `bson:"tMsgRecvByServer" json:"tMsgRecvByServer"`
	RRN                  string             `bson:"rrn" json:"rrn"`
	MerchantId           string             `bson:"-" json:"mid"`
	TerminalID           string             `bson:"-" json:"tid,omitempty"`
	MerchantCategoryCode string             `bson:"-" json:"mcc"`
	MerchantVPA          string             `bson:"-" json:"meVpa"`
	TransactionType      int8               `bson:"transactionType" json:"transactionType"`
	TransactionMode      int8               `bson:"transactionMode" json:"transactionMode"`
	Amount               string             `bson:"txnAmt" json:"txnAmt" binding:"required"`
	TransactionTimeStamp string             `bson:"txnTimeStamp" json:"txnTimestamp"`
	TimeStamp            int64              `bson:"timeStamp" json:"timeStamp"`
	ExpirationTime       int64              `bson:"expirationTime" json:"-"`
	TMsgRecvFromDevice   int64              `bson:"tMsgRecvFromDev" json:"tMsgRecvFromDev"`
	AudioPlayed          uint8              `bson:"audioPlayed" json:"audioPlayed"`
	MessageId            int64              `gorm:"-" json:"id"`
}
