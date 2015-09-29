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
	mongoDbPort := flag.Int("mongoDbPort", -1, "Mongo DB Port")
	awsAccessKey := flag.String("awsAccessKey", "", "AWS access key")
	awsSecretKey := flag.String("awsSecretKey", "", "AWS secret key")
	bucketName := flag.String("bucketName", "", "Bucket name")
	dataFolder := flag.String("dataFolder", "", "Data folder to back up")
	s3Domain := flag.String("s3Domain", "", "The S3 domain")

	flag.Parse()
	return *mongoDbHost, *mongoDbPort, *awsAccessKey, *awsSecretKey, *bucketName, *dataFolder, *s3Domain
}

func printArgs(mongoDbHost string, mongoDbPort int, awsAccessKey string, awsSecretKey string, bucketName string, dataFolder string, s3Domain string) {
	fmt.Println("mongoDbHost  : ", mongoDbHost)
	fmt.Println("mongoDbPort  : ", mongoDbPort)
	fmt.Println("bucketName   : ", bucketName)
	fmt.Println("dataFolder   : ", dataFolder)
	fmt.Println("s3Domain     : ", s3Domain)
}

func abortOnInvalidParams(paramNames []string) {
	for _, paramName := range paramNames {
		fmt.Println(paramName + " is missing or invalid!")
	}
	fmt.Println("Aborting backup operation!")
	os.Exit(1)
}

func validateArgs(mongoDbHost string, mongoDbPort int, awsAccessKey string, awsSecretKey string, bucketName string, dataFolder string, s3Domain string) {
	var invalidArgs []string

	if len(mongoDbHost) < 1 {
		invalidArgs = append(invalidArgs, "mongoDbHost")
	}
	if mongoDbPort < 0 {
		invalidArgs = append(invalidArgs, "mongoDbPort")
	}
	if len(awsAccessKey) < 1 {
		invalidArgs = append(invalidArgs, "awsAccessKey")
	}
	if len(awsSecretKey) < 1 {
		invalidArgs = append(invalidArgs, "awsSecretKey")
	}
	if len(bucketName) < 1 {
		invalidArgs = append(invalidArgs, "bucketName")
	}
	if len(dataFolder) < 1 {
		invalidArgs = append(invalidArgs, "dataFolder")
	}
	if len(s3Domain) < 1 {
		invalidArgs = append(invalidArgs, "s3Domain")
	}

	if len(invalidArgs) > 0 {
		abortOnInvalidParams(invalidArgs)
	}
}

func addtoArchive(path string, fileInfo os.FileInfo, err error) error {
	if fileInfo.IsDir() {
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Cannot open file to add to archive: " + path + ", error: " + err.Error())
		return err
	}
	defer file.Close()

	//create and write tar-specific file header
	fileInfoHeader, err := tar.FileInfoHeader(fileInfo, "")
	if err != nil {
		fmt.Println("Cannot create tar header, error: " + err.Error())
		return err
	}
	//replace file name with full path to preserve file structure in the archive
	fileInfoHeader.Name = path
	err = tarWriter.WriteHeader(fileInfoHeader)
	if err != nil {
		fmt.Println("Cannot write tar header, error: " + err.Error())
		return err
	}

	//add file to the archive
	_, err = io.Copy(tarWriter, file)
	if err != nil {
		fmt.Println("Cannot add file to archive, error: " + err.Error())
		return err
	}

	fmt.Println("Added file " + path + " to archive.")
	return nil
}

func lockDb(session *mgo.Session) error {
	fmt.Println("Attempting to lock DB...")
	err := session.FsyncLock()
	if err != nil {
		return err
	}
	fmt.Println("DB lock command successfully executed.")
	return nil
}

func unlockDb(session *mgo.Session) {
	fmt.Println("Attempting to unlock DB...")
	err := session.FsyncUnlock()
	if err != nil {
		fmt.Println("Cannot unlock DB, operation fails with error: " + err.Error())
		return
	}
	fmt.Println("DB unlock command successfully executed.")
}

func printAbortMessage(operationDescription string, errorMessage string) {
	fmt.Println(operationDescription + ", error: " + errorMessage)
	fmt.Println("Aborting backup operation!")
}

var tarWriter *tar.Writer
var defaultDb = "native-store"
var archiveNameDateFormat = "2006-01-02T15:04:05"

//this enables mgo to connect to secondary nodes
var mongoDirectConnectionConfig = "/?connect=direct"

func main() {
	startTime := time.Now()
	fmt.Println("Starting backup operation: " + startTime.String())

	mongoDbHost, mongoDbPort, awsAccessKey, awsSecretKey, bucketName, dataFolder, s3Domain := readArgs()
	printArgs(mongoDbHost, mongoDbPort, awsAccessKey, awsSecretKey, bucketName, dataFolder, s3Domain)
	validateArgs(mongoDbHost, mongoDbPort, awsAccessKey, awsSecretKey, bucketName, dataFolder, s3Domain)

	mongoConnectionString := mongoDbHost + ":" + strconv.Itoa(mongoDbPort) + mongoDirectConnectionConfig
	session, err := mgo.Dial(mongoConnectionString)
	if err != nil {
		printAbortMessage("Can't connect to mongo on "+mongoConnectionString, err.Error())
		return
	}
	session.SetMode(mgo.Monotonic, true)
	defer session.Close()

	db := session.DB(defaultDb)

	result := make(map[string]interface{})
	err = db.Run(bson.M{"isMaster": 1}, result)
	if err != nil {
		printAbortMessage("Can't check if node is master, db.isMaster() fails", err.Error())
		return
	}

	isMaster := result["ismaster"].(bool)
	if isMaster {
		printAbortMessage("Backup will NOT be performed", "the node I am running on is PRIMARY")
		return
	}
	fmt.Println("The node I am running on is SECONDARY, backup will be performed.")

	err = lockDb(session)
	if err != nil {
		printAbortMessage("Cannot lock DB", err.Error())
		return
	}

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
	gzipWriter := gzip.NewWriter(pipeWriter)
	//create a tar archive
	tarWriter = tar.NewWriter(gzipWriter)

	//recursively walk the filetree of the data folder,
	//adding all files and folder structure to the archive
	go func() {
		//we have to close these here so that the read function doesn't block
		defer pipeWriter.Close()
		defer gzipWriter.Close()
		defer tarWriter.Close()

		err = filepath.Walk(dataFolder, addtoArchive)
		if err != nil {

		}
	}()

	archiveName := time.Now().Format(archiveNameDateFormat)

	//create a writer for the bucket
	bucketWriter, err := bucket.PutWriter(archiveName, nil, nil)
	if err != nil {
		printAbortMessage("PutWriter cannot be created", err.Error())
		return
	}
	defer bucketWriter.Close()

	//upload the archive to the bucket
	_, err = io.Copy(bucketWriter, pipeReader)
	if err != nil {
		printAbortMessage("Cannot upload archive to S3", err.Error())
		return
	}
	pipeReader.Close()

	fmt.Println("Duration: " + time.Since(startTime).String())
}
