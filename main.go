package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"github.com/cheggaaa/pb"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
)

// S3 ...
type S3 struct {
	Bucket string
	Key    string
	Secret string
}

// Config ...
type Config struct {
	Gophers int
	From    S3
	To      S3
}

var (
	config     Config
	fromKeys   []string
	fromKeyMap map[string]s3.Key
	toKeys     []string
	toKeyMap   map[string]s3.Key
	copyKeys   []string
)

func main() {

	gophers := flag.Int("gophers", 10, "how many gophers will move the files (concurrency)")
	flagFromBucket := flag.String("from.bucket", "", "source from which to copy")
	flagFromKey := flag.String("from.key", "", "aws_access_key_id of a user with access to the from.bucket")
	flagFromSecret := flag.String("from.secret", "", "aws_secret_access_key for the from.key user")
	flagToBucket := flag.String("to.bucket", "", "destination to which to copy")
	flagToKey := flag.String("to.key", os.Getenv("AWS_ACCESS_KEY_ID"), "aws_access_key_id of a user with access to the to.bucket")
	flagToSecret := flag.String("to.secret", os.Getenv("AWS_SECRET_ACCESS_KEY"), "aws_secret_access_key for the to.key user")

	flag.Parse()

	if flagFromBucket == nil || flagFromKey == nil || flagFromSecret == nil ||
		flagToBucket == nil || flagToKey == nil || *flagToKey == "" ||
		flagToSecret == nil || *flagToSecret == "" {

		flag.Usage()
		return
	}

	config = Config{
		Gophers: *gophers,
		From:    S3{Bucket: *flagFromBucket, Key: *flagFromKey, Secret: *flagFromSecret},
		To:      S3{Bucket: *flagToBucket, Key: *flagToKey, Secret: *flagToSecret},
	}

	// Read full index of all keys in the from bucket
	fromBucket := getFromBucket()

	fmt.Println("Fetching from bucket index")
	tmpFromKeyMap, err := fromBucket.GetBucketContents()
	handle(err)

	if tmpFromKeyMap == nil || len(*tmpFromKeyMap) == 0 {
		handle(fmt.Errorf("from keys is empty"))
	}
	fromKeyMap = *tmpFromKeyMap

	for k := range fromKeyMap {
		// k = 7b3e1f7b26e4ab1d39511f0a9ad234e450a6a70a
		// v = {
		// 	Key:7b3e1f7b26e4ab1d39511f0a9ad234e450a6a70a
		// 	LastModified:2015-03-09T11:05:16.000Z
		// 	Size:484777
		// 	ETag:"9fc1eaa32632259c417da34d8d8fe6b2"
		// 	StorageClass:STANDARD
		// 	Owner:{
		// 		ID:994669faa182d6117067c662410abcb52b21b202eeb52b419c681c0c99685d5f
		// 		DisplayName:operations
		// 	}
		// }
		fromKeys = append(fromKeys, k)
	}

	// Read full index of all keys in the to bucket
	toBucket := getToBucket()

	fmt.Println("Fetching to bucket index")
	tmpToKeyMap, err := toBucket.GetBucketContents()
	handle(err)
	toKeyMap = *tmpToKeyMap

	if toKeyMap != nil {
		for k := range toKeyMap {
			toKeys = append(toKeys, k)
		}
	}

	// Work out what we are going to copy
	for _, fromKey := range fromKeys {
		from := fromKeyMap[fromKey]

		if to, ok := toKeyMap[fromKey]; ok {
			if from.ETag != to.ETag {
				copyKeys = append(copyKeys, fromKey)
			}
		} else {
			copyKeys = append(copyKeys, fromKey)
		}
	}

	fmt.Printf("%d items in from bucket: %s\n", len(fromKeys), config.From.Bucket)
	fmt.Printf("%d items in to bucket: %s\n", len(toKeys), config.To.Bucket)
	fmt.Printf("%d items to be copied\n", len(copyKeys))

	copyAll()
}

func getFromBucket() (bucket *s3.Bucket) {
	fromS3 := s3.New(
		aws.Auth{
			AccessKey: config.From.Key,
			SecretKey: config.From.Secret,
		},
		aws.EUWest,
	)
	if fromS3 == nil {
		return
	}

	bucket = fromS3.Bucket(config.From.Bucket)

	if bucket == nil {
		handle(fmt.Errorf("could not get bucket"))
	}
	return
}

func getToBucket() (bucket *s3.Bucket) {
	toS3 := s3.New(
		aws.Auth{
			AccessKey: config.To.Key,
			SecretKey: config.To.Secret,
		},
		aws.EUWest,
	)
	if toS3 == nil {
		return
	}

	bucket = toS3.Bucket(config.To.Bucket)

	if bucket == nil {
		handle(fmt.Errorf("could not get bucket"))
	}
	return
}

func copyAll() {
	errs := runTasks(copyKeys, copy, config.Gophers)
	for _, err := range errs {
		fmt.Println(err)
	}
}

func copy(key string) error {
	data, contentType, err := download(key)
	if err != nil {
		return err
	}

	err = upload(key, data, contentType)
	if err != nil {
		return err
	}

	return nil
}

func download(key string) ([]byte, string, error) {
	bucket := getFromBucket()

	resp, err := bucket.GetResponse(key)
	if err != nil {
		return []byte{}, "", err
	}
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, "", err
	}

	return data, contentType, nil
}

func upload(key string, data []byte, contentType string) error {
	bucket := getToBucket()

	err := bucket.Put(key, data, contentType, s3.Private)
	if err != nil {
		return err
	}

	return err
}

func handle(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// runTasks will take a range of []string, some function args, a function and
// the number of gophers to use, and will then process all tasks evenly across
// the number of gophers.
func runTasks(
	ids []string,
	task func(string) error,
	gophers int,
) []error {

	// Progress bar
	bar := pb.StartNew(len(ids))

	// Cancel control
	done := make(chan struct{})
	quit := false

	// IDs to process, sent via channel
	tasks := make(chan string, len(ids)+1)

	errs := []error{}
	var wg sync.WaitGroup

	// No need to have more gophers than we have tasks
	if gophers > len(ids) {
		gophers = len(ids)
	}

	// Only fire up a set number of worker processes
	for i := 0; i < gophers; i++ {
		wg.Add(1)

		go func() {
			for id := range tasks {
				err := doTask(id, task, done)
				if err != nil {
					// Quit as we encountered an error
					if !quit {
						// Closing the done channel will cancel tasks handled by
						// other gophers
						close(done)
						quit = true
					}
					errs = append(
						errs,
						fmt.Errorf("Failed on ID %d : %+v", id, err),
					)
					break
				}
				bar.Increment()
			}
			wg.Done()
		}()
	}

	for _, id := range ids {
		tasks <- id
	}
	close(tasks)

	wg.Wait()
	if !quit {
		close(done)
	}

	bar.Finish()

	return errs
}

// doTask runs a single task and returns the error value (nil or err).
// If the done channel is closed, then this task is cancelled.
func doTask(
	id string,
	task func(string) error,
	done <-chan struct{},
) error {

	select {
	case <-done:
		return fmt.Errorf("task cancelled")
	default:
		return task(id)
	}
}
