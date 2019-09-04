package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Bucket represents the essential information of an data storage Bucket
type Bucket struct {
	name    BucketName
	region  Region
	size    int64
	objects int
}

func (b Bucket) String() string {
	return fmt.Sprintf(
		"Bucket(name=%s, region=%s, size=%s, objects=%d)",
		b.name,
		b.region,
		ReadableByte(b.size),
		b.objects,
	)
}

// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#S3.ListObjects
func main() {
	log.Println(Summarize("circleup-airflow", "us-west-1"))
}

// BucketName represents the name of an S3 bucket
type BucketName string

// Region represents the region of S3
type Region string

// Summarize retrieve a summary of the bucket information
func Summarize(name BucketName, region Region) Bucket {
	bucket := Bucket{
		name:   name,
		region: region,
	}
	objects, err := AllObjects(
		bucket.name,
		bucket.region,
	)

	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	bucket.size = TotalSize(objects)
	bucket.objects = len(objects)

	return bucket
}

// TotalSize sums up the total size of all the objects
func TotalSize(objects []*s3.Object) int64 {
	var totalSize int64
	for _, o := range objects {
		totalSize += *o.Size
	}
	return totalSize
}

// ReadableByte converts a size in bytes to a human-readable format
func ReadableByte(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// AllObjects returns all the S3 objects in the bucket of the region
func AllObjects(bucket BucketName, region Region) ([]*s3.Object, error) {
	var allObjects []*s3.Object

	svc := s3.New(session.New(), &aws.Config{
		Region: aws.String(string(region)),
	})

	input := &s3.ListObjectsInput{
		Bucket: aws.String(string(bucket)),
	}

	result, err := svc.ListObjects(input)
	if err != nil {
		return nil, err
	}
	allObjects = append(allObjects, result.Contents...)

	for *result.IsTruncated {
		log.Println("Last key:", *result.Contents[len(result.Contents)-1].Key)

		input = &s3.ListObjectsInput{
			Bucket: aws.String(string(bucket)),
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
