package main

import (
	"log"
	"strconv"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type MongoService struct {
	connectionString, database string
	session                    *mgo.Session
}

func buildMongoConnectionString(host string, port int, connectionOptions []string) string {
	mongoConnectionString := host + ":" + strconv.Itoa(port)
	if len(connectionOptions) == 0 {
		return mongoConnectionString
	}

	mongoConnectionString += "/?"
	for _, param := range connectionOptions {
		mongoConnectionString += param + connectionOptionSeparator
	}
	mongoConnectionString = strings.TrimSuffix(mongoConnectionString, connectionOptionSeparator)

	return mongoConnectionString
}

func newMongoService(host string, port int, connectionOptions []string, database string) *MongoService {
	return &MongoService{buildMongoConnectionString(host, port, connectionOptions), database, nil}
}

func (service *MongoService) openSession() {
	session, err := mgo.Dial(service.connectionString)
	if err != nil {
		log.Panic("Can't connect to mongo on "+service.connectionString, err.Error(), err)
	}
	session.SetMode(mgo.Monotonic, true)
	service.session = session
}

func (service *MongoService) closeSession() {
	service.session.Close()
}

func (service *MongoService) isCurrentNodeMaster() bool {
	db := service.session.DB(defaultDb)
	result := make(map[string]interface{})
	err := db.Run(bson.M{"isMaster": 1}, result)
	if err != nil {
		log.Panic("Can't check if node is master, db.isMaster() fails", err.Error(), err)
	}
	return result["ismaster"].(bool)
}

func (service *MongoService) lockDb() {
	info.Println("Attempting to LOCK DB...")
	err := service.session.FsyncLock()
	if err != nil {
		log.Panic("Cannot LOCK DB: "+err.Error(), err)
	}
	info.Println("DB LOCK command successfully executed.")
}

func (service *MongoService) unlockDb() {
	info.Println("Attempting to UNLOCK DB...")
	err := service.session.FsyncUnlock()
	if err != nil {
		log.Panic("Cannot LOCK DB: "+err.Error(), err)
	}
	info.Println("DB UNLOCK command successfully executed.")
}
