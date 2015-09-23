package main

import (
	"fmt"
    "gopkg.in/mgo.v2"
	"strconv"
	"time"
)

func main() {
    startTime := time.Now()
    fmt.Println("Starting backup operation: " + startTime.String())
    
    //$HOSTNAME in the unit file
    //mongoDbHost := "ip-172-23-53-64.eu-west-1.compute.internal"
    mongoDbHost := "document-store-db-01-pr-uk-t.svc.ft.com"
    //export MONGODB_PORT=${docker ps | grep mongodb | awk '{ print $1 }' | xargs -i docker port '{}' 27017 | cut -d":" -f2};
    //mongoDbPort := "27018"
    mongoDbPort := 27017
    
    fmt.Println("Mongo DB host: " + mongoDbHost)
    fmt.Println("Mongo DB port: " + strconv.Itoa(mongoDbPort))
    session, err := mgo.Dial(mongoDbHost + ":" + strconv.Itoa(mongoDbPort))
	//TODO error handling?
    if err != nil {
		panic(err)
	}
	defer session.Close()
    
    fmt.Println("Duration: " + time.Since(startTime).String())
}