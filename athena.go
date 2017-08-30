package athena_s3

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
)

// Amazon API guide: http://docs.aws.amazon.com/sdk-for-go/api/
type Athena struct {
	C *athena.Athena
}

// Create New Athena Client with accessKey , secretAccessKey, region info
func NewAthenaClient(k string, sk string, r string) (*Athena, error) {

	creds := credentials.NewStaticCredentials(k, sk, "")
	_, err := creds.Get()
	if err != nil {
		return nil, fmt.Errorf("Init S3 Client Error: %s", err)
	}

	cfg := aws.NewConfig().WithRegion(r).WithCredentials(creds)

	sess := session.Must(session.NewSession(cfg))

	return &Athena{
		C: athena.New(sess),
	}, nil
}

// Submit Query Request
// db: target db to query
// query: standard sql
// o : output location. s3 output path
func (a *Athena) SubmitAthenaQuery(db *string, query *string, o *string) (*string,error) {

	// Set Database to query
	queryExecutionContext := &athena.QueryExecutionContext{Database: db}

	// Set results of the query
	resultConfiguration := &athena.ResultConfiguration{OutputLocation: o}

	// Create the StartQueryExecutionRequest to send to Athena which will start the query.
	startQueryExecutionInput := &athena.StartQueryExecutionInput{
		QueryExecutionContext: queryExecutionContext,
		ResultConfiguration: resultConfiguration,
		QueryString: query,
	}

	startQueryExecutionOutput,err := a.C.StartQueryExecution(startQueryExecutionInput)

	if err != nil {
		return nil, fmt.Errorf("Submit Query To Athena Error: %v", err)
	}

	return startQueryExecutionOutput.QueryExecutionId, nil
}

// Wait for an Athena query to complete, fail or is canceled.
// If a query fails or is canceled, then Error
// Query finish will return nil
func (a *Athena) WaitForQueryToComplete(queryExecutionId *string) error {

	getQueryExecutionInput := &athena.GetQueryExecutionInput{
		QueryExecutionId: queryExecutionId,
	}

	isQueryStillRunning := true

	for isQueryStillRunning  {

		getQueryExecutionOutput,err :=  a.C.GetQueryExecution(getQueryExecutionInput)

		if err != nil {
			return fmt.Errorf("Athena Query was cancelled. queryExecutionId = " + *queryExecutionId)
		}

		queryState := getQueryExecutionOutput.QueryExecution.Status.State

		switch *queryState {
		case "SUCCEEDED" :
			isQueryStillRunning = false
		case "CANCELLED":
			return fmt.Errorf("Athena Query was cancelled. queryExecutionId = " + *queryExecutionId)
		case "FAILED":
			return fmt.Errorf("Athena Query Failed to run with Error Message:  " + *getQueryExecutionOutput.QueryExecution.Status.StateChangeReason)
		}
	}

	return nil
}

// Define function for executing data
type ProcessRow func(page *athena.GetQueryResultsOutput, lastPage bool) bool
func (a *Athena) ProcessResultRows(queryExecutionId *string, fn ProcessRow) error {

	// Get max 1000 record for each query
	maxResult := new(int64)
	*maxResult = 1000

	getQueryResultsInput := &athena.GetQueryResultsInput{
		MaxResults: maxResult,
		QueryExecutionId: queryExecutionId,
	}

	err := a.C.GetQueryResultsPages(getQueryResultsInput, fn)
	if err != nil {
		return fmt.Errorf("Athena Query Result Process Error. queryExecutionId = " + *queryExecutionId)
	}

	return nil
}