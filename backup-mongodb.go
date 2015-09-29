package main

import (
	"archive/tar"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/rlmcpherson/s3gof3r"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
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

func lockDb(session *mgo.Session) {
	fmt.Println("Attempting to lock DB...")
	session.FsyncLock()
	//TODO check result
	fmt.Println("DB lock command successfully executed.")
}

func unlockDb(session *mgo.Session) {
	fmt.Println("Attempting to unlock DB...")
	session.FsyncUnlock()
	//TODO check result
	fmt.Println("DB unlock command successfully executed.")
}

var tarfileWriter *tar.Writer
var defaultDb = "native-store"

//this enables mgo to connect to secondary nodes
var mongoDirectConnectionConfig = "/?connect=direct"

//TODO error handling
func main() {
	startTime := time.Now()
	fmt.Println("Starting backup operation: " + startTime.String())

	mongoDbHost, mongoDbPort, awsAccessKey, awsSecretKey, bucketName, dataFolder, s3Domain := readArgs()
	printArgs(mongoDbHost, mongoDbPort, awsAccessKey, awsSecretKey, bucketName, dataFolder, s3Domain)

	session, _ := mgo.Dial(mongoDbHost + ":" + strconv.Itoa(mongoDbPort) + mongoDirectConnectionConfig)
	session.SetMode(mgo.Monotonic, true)
	defer session.Close()
	db := session.DB(defaultDb)

	result := make(map[string]interface{})

	db.Run(bson.M{"isMaster": 1}, result)
	isMaster := result["ismaster"].(bool)

	if isMaster {
		fmt.Println("The node I am running on is PRIMARY, backup will NOT be performed.")
		return
	}

	fmt.Println("The node I am running on is SECONDARY, backup will be performed.")

	lockDb(session)
	defer unlockDb(session)

	//the default domain is s3.amazonaws.com, we need the eu-west domain
	s3gof3r.DefaultDomain = s3Domain

	awsKeys := s3gof3r.Keys{
		AccessKey: awsAccessKey,
		SecretKey: awsSecretKey,
	}

	s3 := s3gof3r.New("", awsKeys)
	bucket := s3.Bucket(bucketName)
	pipeReader, pipeWriter := io.Pipe()

	//compress the tar archive
	fileWriter := gzip.NewWriter(pipeWriter)

	//create a tar archive
	tarfileWriter = tar.NewWriter(fileWriter)

	//recursively walk the filetree of the data folder,
	//adding all files and folder structure to the archive
	go func() {
		//we have to close these here so that the read function doesn't block
		defer pipeWriter.Close()
		defer fileWriter.Close()
		defer tarfileWriter.Close()

		filepath.Walk(dataFolder, addtoArchive)
	}()

	archiveName := time.Now().Format("2006-01-02T15:04:05")

	//create a writer for the bucket
	bucketWriter, _ := bucket.PutWriter(archiveName, nil, nil)
	defer bucketWriter.Close()

	//upload the archive to the bucket
	io.Copy(bucketWriter, pipeReader)
	defer pipeReader.Close()

	fmt.Println("Duration: " + time.Since(startTime).String())
}
