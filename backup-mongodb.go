package main

import (
	"archive/tar"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/rlmcpherson/s3gof3r"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func readArgs() (string, int, string, string, string, string, string) {
	mongoDbHost := flag.String("mongoDbHost", "", "Mongo DB Host")
	mongoDbPort := flag.Int("mongoDbPort", 27017, "Mongo DB Port")
	awsAccessKey := flag.String("awsAccessKey", "", "AWS access key")
	awsSecretKey := flag.String("awsSecretKey", "", "AWS secret key")
	bucketName := flag.String("bucketName", "", "Bucket name")
	dataFolder := flag.String("dataFolder", "", "Data folder to back up")
	s3Domain := flag.String("s3Domain", "", "The S3 domain")

	flag.Parse()
	return *mongoDbHost, *mongoDbPort, *awsAccessKey, *awsSecretKey, *bucketName, *dataFolder, *s3Domain
}

func printArgs(mongoDbHost string, mongoDbPort int, awsAccessKey string, awsSecretKey string, bucketName string, dataFolder string, s3Domain string) {
	fmt.Println("mongoDbHost:", mongoDbHost)
	fmt.Println("mongoDbPort", mongoDbPort)
	fmt.Println("awsAccessKey", awsAccessKey)
	fmt.Println("awsSecretKey", awsSecretKey)
	fmt.Println("bucketName", bucketName)
	fmt.Println("dataFolder", dataFolder)
	fmt.Println("s3Domain", s3Domain)
}

func addtoArchive(path string, fileInfo os.FileInfo, err error) error {
	if fileInfo.IsDir() {
		return nil
	}

	file, _ := os.Open(path)
	defer file.Close()

	//create and write tar-specific file header
	fileInfoHeader, _ := tar.FileInfoHeader(fileInfo, "")
	//replace file name with full path to preserve file structure in the archive
	fileInfoHeader.Name = path
	tarfileWriter.WriteHeader(fileInfoHeader)

	//add file to the archive
	io.Copy(tarfileWriter, file)
	fmt.Println("Added file " + path + " to archive.")
	return nil
}

var tarfileWriter *tar.Writer
var defaultDb = "native-store"

//TODO error handling
func main() {
	startTime := time.Now()
	fmt.Println("Starting backup operation: " + startTime.String())

	mongoDbHost, mongoDbPort, awsAccessKey, awsSecretKey, bucketName, dataFolder, s3Domain := readArgs()
	printArgs(mongoDbHost, mongoDbPort, awsAccessKey, awsSecretKey, bucketName, dataFolder, s3Domain)

	session, _ := mgo.Dial(mongoDbHost + ":" + strconv.Itoa(mongoDbPort))
	defer session.Close()
	db := session.DB(defaultDb)

	result := make(map[string]interface{})

	db.Run(bson.M{"isMaster": 1}, result)
	isMaster := result["ismaster"].(bool)

	if isMaster {
		fmt.Println("The node I am running on is master, backup will not take place")
		//os.Exit(2)
	}

	//db.Run(bson.M{"db.fsyncLock": 1}, result)
	//TODO log if this fails
	//defer db.Run(bson.M{"db.fsyncUnlock": 1}, result)

	//the default domain is s3.amazonaws.com, we need the eu-west domain
	s3gof3r.DefaultDomain = s3Domain

	awsKeys := s3gof3r.Keys{
		AccessKey: awsAccessKey,
		SecretKey: awsSecretKey,
	}

	s3 := s3gof3r.New("", awsKeys)
	bucket := s3.Bucket(bucketName)

	//TODO generate filename based on date
	destinationfileName := "2015-Sep-28.tar.gz"

	pipeReader, pipeWriter := io.Pipe()

	//compress the tar archive
	fileWriter := gzip.NewWriter(pipeWriter)
	
	//create a tar archive
	tarfileWriter = tar.NewWriter(fileWriter)

	//recursively walk the filetree of the data folder,
	//adding all files and folder structure to the archive
	go func() {
		//we have to close this here so that the read function completes
		defer pipeWriter.Close()
		defer fileWriter.Close()
		defer tarfileWriter.Close()
		filepath.Walk(dataFolder, addtoArchive)
	} ()
	
	//create a writer for the bucket
	bucketWriter, _ := bucket.PutWriter(destinationfileName, nil, nil)
	defer bucketWriter.Close()

	//upload the archive to the bucket
	io.Copy(bucketWriter, pipeReader)
	defer pipeReader.Close()
	
	fmt.Println("Duration: " + time.Since(startTime).String())
}
