package athena_s3

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"os"
	"bytes"
	"net/http"
)

// Amazon API guide: http://docs.aws.amazon.com/sdk-for-go/api/
type S3 struct {
	C *s3.S3
}

// Create New S3 Client with accessKey , secretAccessKey, region info
func NewS3(k string, sk string, r string) (*S3, error) {

	creds := credentials.NewStaticCredentials(k, sk, "")
	_, err := creds.Get()
	if err != nil {
		return nil, fmt.Errorf("Init S3 Client Error: %s", err)
	}

	cfg := aws.NewConfig().WithRegion(r).WithCredentials(creds)

	sess := session.Must(session.NewSession(cfg))

	return &S3{
		C: s3.New(sess),
	}, nil
}

// Upload file from local to s3
// Local file path: srcPath
// S3 bucket : bucket
// S3 destination path: desPath
func (c *S3) Upload(srcPath string, bucket string, desPath string) error {

	file, err := os.Open(srcPath)
	if err != nil {
		return err
	}

	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)

	// read file content to buffer
	file.Read(buffer)

	fileBytes := bytes.NewReader(buffer)
	fileType := http.DetectContentType(buffer)

	params := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key: aws.String(desPath),
		Body: fileBytes,
		ContentLength: aws.Int64(size),
		ContentType: aws.String(fileType),
	}
	
	_, err = c.C.PutObject(params)
	if err != nil {
		return err
	}

	return nil
}