package main

import (
	"archive/tar"
	"compress/gzip"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

var tarWriter *tar.Writer
var info *log.Logger
var warn *log.Logger

//this enables mgo to connect to secondary nodes
const mongoDirectConnectionOption = "connect=direct"
const logPattern = log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile | log.LUTC
const defaultDb = "native-store"
const connectionOptionSeparator = "&"
const archiveNameDateFormat = "2006-01-02T15-04-05"

func main() {
	initLogs(os.Stdout, os.Stdout, os.Stderr)

	startTime := time.Now()
	info.Println("Starting backup operation.")

	mongoDbHost, mongoDbPort, awsAccessKey, awsSecretKey, bucketName, dataFolder, s3Domain := readArgs()
	printArgs(mongoDbHost, mongoDbPort, awsAccessKey, awsSecretKey, bucketName, dataFolder, s3Domain)
	checkIfArgsAreEmpty(mongoDbHost, mongoDbPort, awsAccessKey, awsSecretKey, bucketName, dataFolder, s3Domain)

	dbService := newMongoService(mongoDbHost, mongoDbPort, []string{mongoDirectConnectionOption}, defaultDb)
	dbService.openSession()
	defer dbService.closeSession()

	if dbService.isCurrentNodeMaster() {
		log.Panic("Backup will NOT be performed", "the node I am running on is PRIMARY")
	}
	info.Println("The node I am running on is SECONDARY, backup will be performed.")

	dbService.lockDb()
	defer dbService.unlockDb()

	pipeReader, pipeWriter := io.Pipe()
	//compress the tar archive
	gzipWriter := gzip.NewWriter(pipeWriter)
	//create a tar archive
	tarWriter = tar.NewWriter(gzipWriter)

	//a goroutine is needed because the pipe is synchronous:
	//the writer will block until the reader is reading and vice-versa
	go func() {
		//we have to close these here so that the read function doesn't block
		defer pipeWriter.Close()
		defer gzipWriter.Close()
		defer tarWriter.Close()

		//recursively walk the filetree of the data folder,
		//writing all files and folder structure to the archive
		filepath.Walk(dataFolder, addtoArchive)
	}()

	archiveName := time.Now().UTC().Format(archiveNameDateFormat)
	bucketWriterProvider := newS3WriterProvider(awsAccessKey, awsSecretKey, s3Domain, bucketName)

	bucketWriter, err := bucketWriterProvider.getWriter(archiveName)
	if err != nil {
		log.Panic("BucketWriter cannot be created: "+err.Error(), err)
		return
	}
	defer bucketWriter.Close()

	//upload the archive to the bucket
	_, err = io.Copy(bucketWriter, pipeReader)
	if err != nil {
		log.Panic("Cannot upload archive to S3: "+err.Error(), err)
		return
	}
	pipeReader.Close()

	info.Println("Uploaded archive " + archiveName + " to " + bucketName + " S3 bucket.")
	info.Println("Duration: " + time.Since(startTime).String())
}

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
	info.Println("Using arguments:")
	info.Println("mongoDbHost  : ", mongoDbHost)
	info.Println("mongoDbPort  : ", mongoDbPort)
	info.Println("bucketName   : ", bucketName)
	info.Println("dataFolder   : ", dataFolder)
	info.Println("s3Domain     : ", s3Domain)
}

func abortOnInvalidParams(paramNames []string) {
	for _, paramName := range paramNames {
		warn.Println(paramName + " is missing or invalid!")
	}
	log.Panic("Aborting backup operation!")
}

func checkIfArgsAreEmpty(mongoDbHost string, mongoDbPort int, awsAccessKey string, awsSecretKey string, bucketName string, dataFolder string, s3Domain string) {
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
		log.Panic("Cannot open file to add to archive: "+path+", error: "+err.Error(), err)
	}
	defer file.Close()

	//create and write tar-specific file header
	fileInfoHeader, err := tar.FileInfoHeader(fileInfo, "")
	if err != nil {
		log.Panic("Cannot create tar header, error: "+err.Error(), err)
	}
	//replace file name with full path to preserve file structure in the archive
	fileInfoHeader.Name = path
	err = tarWriter.WriteHeader(fileInfoHeader)
	if err != nil {
		log.Panic("Cannot write tar header, error: "+err.Error(), err)
	}

	//add file to the archive
	_, err = io.Copy(tarWriter, file)
	if err != nil {
		log.Panic("Cannot add file to archive, error: "+err.Error(), err)
	}

	info.Println("Added file " + path + " to archive.")
	return nil
}

func initLogs(infoHandle io.Writer, warnHandle io.Writer, panicHandle io.Writer) {
	//to be used for INFO-level logging: info.Println("foor is now bar")
	info = log.New(infoHandle, "INFO  - ", logPattern)
	//to be used for WARN-level logging: info.Println("foor is now bar")
	warn = log.New(warnHandle, "WARN  - ", logPattern)

	//to be used for panics: log.Panic("foo is on fire")
	//log.Panic() = log.Printf + panic()
	log.SetFlags(logPattern)
	log.SetPrefix("ERROR - ")
	log.SetOutput(panicHandle)
}
