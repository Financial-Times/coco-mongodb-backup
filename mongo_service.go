package main

import (
	"log"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type MongoService struct {
	connectionString, database string
	session                    *mgo.Session
}

type mongoClusterHostInfo struct {
	node        string
	primary     string
	secondaries []string
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

func (service *MongoService) isNodeForBackup() bool {
	db := service.session.DB(defaultDb)
	result := make(map[string]interface{})
	err := db.Run(bson.M{"isMaster": 1}, result)
	if err != nil {
		log.Panic("Can't check if node is master, db.isMaster() fails", err.Error(), err)
	}
	backup, hostInfo := isNodeForBackup(result)

	if !backup {
		if hostInfo.node == hostInfo.primary {
			info.Println("This node is master.")
		} else {
			info.Printf("This node (%s) is not the lowest secondary (%s).", hostInfo.node, hostInfo.secondaries[0])
		}
	}

	return backup
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

func isNodeForBackup(mongoJson map[string]interface{}) (bool, mongoClusterHostInfo) {
	if mongoJson["me"] == nil || mongoJson["hosts"] == nil {
		warn.Println("Node is not part of a cluster.")
		return false, mongoClusterHostInfo{}
	}

	master := mongoJson["ismaster"].(bool)

	thisNode := mongoJson["me"].(string)
	primary := mongoJson["primary"].(string)

	allNodesUntyped := mongoJson["hosts"].([]interface{})
	clusterSize := len(allNodesUntyped)
	allNodes := make([]string, 0, clusterSize)
	for _, v := range allNodesUntyped {
		node := v.(string)
		if node != primary {
			allNodes = append(allNodes, node)
		}
	}
	sort.Strings(allNodes)
	lowestSecondary := allNodes[0]

	return (!master) && (lowestSecondary == thisNode),
		mongoClusterHostInfo{thisNode, primary, allNodes}
}
