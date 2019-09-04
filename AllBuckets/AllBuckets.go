package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Using the aws API to programmatically find out the buckets we have
// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#example_S3_listBuckets_shared00
//
func main() {
	buckets, err := listBuckets("us-west-1")
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	for _, b := range buckets {
		fmt.Println(b)
	}
}

func mapSlice(vs []*s3.Bucket, f func(*s3.Bucket) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

func bucketToName(b *s3.Bucket) string {
	return *(b.Name)
}

func listBuckets(region string) ([]string, error) {
	svc := s3.New(session.New(), &aws.Config{
		Region: aws.String(region), // US West (N. California)
	})
	input := &s3.ListBucketsInput{}

	result, err := svc.ListBuckets(input)
	if err != nil {
		return nil, err
	}

	return mapSlice(result.Buckets, bucketToName), nil
}
