# Mongo DB backup application

**Locks the DB it's run on, archives and compresses the given data folder, uploads it to an AWS S3 bucket, and unlocks the DB.**

## Installation

For getting started with Go, refer to the [nativerw README](https://github.com/Financial-Times/nativerw/blob/master/README.md)

`go install github.com/Financial-Times/coco-mongodb-backup`

:information_source: Needs GO 1.5 or higher

## Running the Go executable

```
 coco-mongodb-backup \
    -mongoDbPort=27017 \
    -mongoDbHost=mongo01.host \
    -awsAccessKey=xxx \
    -awsSecretKey=xxxx \
    -bucketName=test \
    -dataFolder=/tmp/dir \
    -s3Domain=s3-eu-west-1.amazonaws.com \
    -env=test
```

### Example output

```
INFO  - 2015/10/07 14:15:45.539330 backup-mongodb.go:29: Starting backup operation.
INFO  - 2015/10/07 14:15:45.539394 backup-mongodb.go:102: Using arguments:
INFO  - 2015/10/07 14:15:45.539399 backup-mongodb.go:103: mongoDbHost  :  mongo01.host
INFO  - 2015/10/07 14:15:45.539402 backup-mongodb.go:104: mongoDbPort  :  27017
INFO  - 2015/10/07 14:15:45.539405 backup-mongodb.go:105: bucketName   :  test
INFO  - 2015/10/07 14:15:45.539408 backup-mongodb.go:106: dataFolder   :  /tmp/dir/
INFO  - 2015/10/07 14:15:45.539411 backup-mongodb.go:107: s3Domain     :  s3-eu-west-1.amazonaws.com
INFO  - 2015/10/07 14:15:45.679812 backup-mongodb.go:108: env          :  test
INFO  - 2015/10/07 14:15:45.805473 backup-mongodb.go:42: The node I am running on is SECONDARY, backup will be performed.
INFO  - 2015/10/07 14:15:45.805490 MongoService.go:60: Attempting to LOCK DB...
INFO  - 2015/10/07 14:15:45.892440 MongoService.go:65: DB LOCK command successfully executed.
INFO  - 2015/10/07 14:15:46.218659 backup-mongodb.go:176: Added file /tmp/dir/tmp_1 to archive.
INFO  - 2015/10/07 14:15:46.302831 backup-mongodb.go:176: Added file /tmp/dir/data1.db to archive.
INFO  - 2015/10/07 14:15:46.445783 backup-mongodb.go:176: Added file /tmp/dir/data2.db to archive.
INFO  - 2015/10/07 14:15:46.445890 backup-mongodb.go:176: Added file /tmp/dir/journal.1 to archive.
INFO  - 2015/10/07 14:15:46.447363 backup-mongodb.go:176: Added file /tmp/dir/journal.2 to archive.
INFO  - 2015/10/07 14:15:46.448399 backup-mongodb.go:84: Uploaded archive 2015-10-07T14-15-45_test to test S3 bucket.
INFO  - 2015/10/07 14:15:46.448407 backup-mongodb.go:85: Duration: 909.089342ms
INFO  - 2015/10/07 14:15:48.069550 MongoService.go:69: Attempting to UNLOCK DB...
INFO  - 2015/10/07 14:15:48.105871 MongoService.go:74: DB UNLOCK command successfully executed.
```

## Building docker

`docker build -t coco/coco-mongodb-backup .`

## Running the Docker container

```
docker run \
--env "MONGODB_PORT=27017" \
--env="MONGODB_HOST=mongo01.host"  \
--env "AWS_ACCESS_KEY=xxx" \
--env "AWS_SECRET_KEY=xxxx" \
--env "BUCKET_NAME=test" \
--env "DATA_FOLDER=/data/db/" \
--env "S3_DOMAIN=s3-eu-west-1.amazonaws.com" \
--env "ENV_TAG=test" \
-v /tmp/dir/:/data/db  \
coco/coco-mongodb-backup
```

## Implementation details
### Gof3r
Uses [gof3r](https://github.com/rlmcpherson/s3gof3r) to stream the archived folder to S3.
### Go Pipe
Uses a [pipe](https://golang.org/pkg/io/#Pipe) to transfer the output of the archiver straight to S3 - this means that the archived folder itself isn't stored on the disk.
