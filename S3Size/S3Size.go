package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// BucketName represents the name of an S3 bucket
type BucketName string

// Region represents the region of S3
type Region string

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
	const region = "us-west-1"

	bucketNames, err := AllBuckets(region)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	log.Printf("Processing %d buckets\n", len(bucketNames))
	buckets := []Bucket{}
	for i, bn := range bucketNames {
		b := Summarize(BucketName(bn), region)
		log.Printf("%d) %s\n", i, b)
		buckets = append(buckets, b)
	}

	log.Printf(
		"Total Size of all %d buckets is: %s\n",
		len(buckets),
		ReadableByte(totalBucketsSize(buckets)),
	)
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

// AllBuckets fetches all the buckets in the region
func AllBuckets(region Region) ([]string, error) {
	svc := s3.New(session.New(), &aws.Config{
		Region: aws.String(string(region)),
	})
	input := &s3.ListBucketsInput{}

	result, err := svc.ListBuckets(input)
	if err != nil {
		return nil, err
	}

	return mapSlice(result.Buckets, bucketToName), nil
}

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
		return bucket
	}

	bucket.size = totalSizeObjects(objects)
	bucket.objects = len(objects)

	return bucket
}

func totalBucketsSize(buckets []Bucket) int64 {
	var sum int64
	for _, b := range buckets {
		sum += b.size
	}
	return sum
}

// totalSizeObjects sums up the total size of all the objects
func totalSizeObjects(objects []*s3.Object) int64 {
	var sum int64
	for _, o := range objects {
		sum += *o.Size
	}
	return sum
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
	log.Printf("Retrieving '%s' from '%s'\n", bucket, region)

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
		lastKey := *result.Contents[len(result.Contents)-1].Key
		// log.Println("Last key:", lastKey)

		input = &s3.ListObjectsInput{
			Bucket: aws.String(string(bucket)),
			Marker: aws.String(lastKey),
		}

		result, err = svc.ListObjects(input)
		if err != nil {
			return allObjects, err
		}
		allObjects = append(allObjects, result.Contents...)
	}

	return allObjects, nil
}
