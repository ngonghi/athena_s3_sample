package main

import (
	"github.com/ngonghi/athena_s3_sample"
	"github.com/aws/aws-sdk-go/service/athena"
	"fmt"
)

func main() {

	// S3 Upload Example
	s3,err := athena_s3.NewS3("KEY", "SECRET_KEY", "REGION")

	if err != nil {
		panic(err)
	}
	
	err = s3.Upload("/tmp/20170801_2.csv", "butket_name", "test.csv")

	if err != nil {
		panic(err)
	}


	// Athena Query Example
	athena, err := athena_s3.NewAthenaClient("KEY", "SECRET_KEY", "REGION")
	if err != nil {
		panic(err)
	}

	sql := "SELECT * FROM TEST"
	db := "TEST"
	output := "s3://TEST"

	queryId, err := athena.SubmitAthenaQuery(&db, &sql, &output)
	if err != nil {
		panic(err)
	}

	err = athena.WaitForQueryToComplete(queryId)
	if err != nil {
		panic(err)
	}

	err = athena.ProcessResultRows(queryId, executeResult)
	if err != nil {
		panic(err)
	}
}

// Execute Data From Athena
func executeResult(output *athena.GetQueryResultsOutput, lastPage bool)  bool {
	fmt.Println("Execute Data From Athena")
	return true
}