package main

import (
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#S3.ListObjects
func main() {
	objects, err := listObjects(
		"circleup-airflow",
		"us-west-1", // US West (N. California)
	)

	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	var totalSize int64
	for _, o := range objects {
		totalSize += *o.Size
	}

	log.Println("Total size:", totalSize, "Bytes")
	log.Println("Total objects:", len(objects))
}

func listObjects(bucket string, region string) ([]*s3.Object, error) {
	var allObjects []*s3.Object

	svc := s3.New(session.New(), &aws.Config{
		Region: aws.String(region),
	})

	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	}

	result, err := svc.ListObjects(input)
	if err != nil {
		return nil, err
	}
	allObjects = append(allObjects, result.Contents...)

	for *result.IsTruncated {
		log.Println("Last key:", *result.Contents[len(result.Contents)-1].Key)

		input = &s3.ListObjectsInput{
			Bucket: aws.String(bucket),
			Marker: aws.String(*result.Contents[len(result.Contents)-1].Key),
		}

		result, err = svc.ListObjects(input)
		if err != nil {
			return allObjects, err
		}
		allObjects = append(allObjects, result.Contents...)
	}

	return allObjects, nil
}
