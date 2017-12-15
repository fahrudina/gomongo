package main

import (
	"strings"

	"github.com/helioina/api/data"
	"github.com/helioina/api/log"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type MongoDB struct {
	Session     *mgo.Session
	Attachments *mgo.Collection
	Embeddeds   *mgo.Collection
	Users       *mgo.Collection
}

type MongoBlob struct {
	Session  *mgo.Session
	Database *mgo.GridFS
	Files    *mgo.Collection
	Chunks   *mgo.Collection
}

type Attachment struct {
	Id               string `json:"id,omitempty"`
	Body             string `json:"body,omitempty"`
	UrlPath          string `json:"urlpath,omitempty"`
	FileName         string `json:"filename,omitempty"`
	ContentType      string `json:"contenttype,omitempty"`
	Charset          string `json:"charset,omitempty"`
	MIMEVersion      string `json:"mimeversion,omitempty"`
	TransferEncoding string `json:"transferencoding,omitempty"`
	Size             int    `json:"size,omitempty"`
}

type Embedded struct {
	Id               string `json:"id,omitempty"`
	FilePath         string `json:"filepath,omitempty"`
	UrlPath          string `json:"urlpath,omitempty"`
	FilePathTemp     string `json:"filepathtemp,omitempty"`
	UrlPathTemp      string `json:"urlpathtemp,omitempty"`
	FolderPath       string `json:"folderpath,omitempty"`
	FileName         string `json:"filename,omitempty"`
	ContentType      string `json:"contenttype,omitempty"`
	TransferEncoding string `json:"transferencoding,omitempty"`
	CidInline        string `json:"cidinline,omitempty"`
	Size             int    `json:"size,omitempty"`
}

var (
	mgoSession *mgo.Session
)

func CreateMongoDB() *MongoDB {

	session, err := mgo.Dial("127.0.0.1:27017")

	/*info := &mgo.DialInfo{
		Addrs: []string{c.MongoUri},
		//Timeout:  60 * time.Second,
		Database: c.MongoDbName, //c.MongoDb,
		Username: c.MongoDbUsername,
		Password: c.MongoDbPassword,
	}

	session, err := mgo.DialWithInfo(info)*/

	if err != nil {
		log.LogError("Error connecting to MongoDB: %s", err)
		return nil
	}

	session.SetMode(mgo.Monotonic, true)

	return &MongoDB{
		Session:     session,
		Attachments: session.DB("Smtpd").C("Attachments"),
		Embeddeds:   session.DB("Smtpd").C("Embeddeds"),
		Users:       session.DB("Smtpd").C("Users"),
	}
}

func CreateMongoBlob() *MongoBlob {
	log.LogTrace("Connecting to MongoDB \n")

	session, err := mgo.Dial("127.0.0.1:27017")

	/*info := &mgo.DialInfo{
		Addrs: []string{c.MongoBlobUri},
		//Timeout:  60 * time.Second,
		Database: c.MongoDbName, //c.MongoDb,
		Username: c.MongoDbUsername,
		Password: c.MongoDbPassword,
	}

	session, err := mgo.DialWithInfo(info)*/

	if err != nil {
		log.LogError("Error connecting to MongoDB: %s", err)
		return nil
	}

	return &MongoBlob{
		Session:  session,
		Database: session.DB("FileSystem").GridFS("fs"),
		Files:    session.DB("FileSystem").C("fs.files"),
		Chunks:   session.DB("FileSystem").C("fs.chunks"),
	}
}

func (mongo *MongoDB) Close() {
	mongo.Session.Close()
}

func main() {

	mongoData := CreateMongoDB()
	mongoBlob := CreateMongoBlob()
	updateAttachment(mongoData, mongoBlob) //--> For Update Owners Attachment
	updateEmbeded(mongoData, mongoBlob)    //--> For Update Owners Embeded

}

func updateEmbeded(mongoData *MongoDB, mongoBlob *MongoBlob) {
	result := []Embedded{}
	if err := mongoData.Embeddeds.Find(bson.M{}).Select(bson.M{"id": 1, "filepath": 1}).All(&result); err != nil {
		log.LogError("%v", err.Error())
	}
	for _, v := range result {
		splits := strings.Split(v.FilePath, "/")
		emailUser := mongoData.getEmailUser(splits[1])
		for _, ve := range emailUser {
			log.LogTrace("<%s>\n", ve.Email)
			mongoBlob.updateMetadata(v.Id, ve.Email)
		}
	}
}

func updateAttachment(mongoData *MongoDB, mongoBlob *MongoBlob) {
	result := []Attachment{}
	if err := mongoData.Attachments.Find(bson.M{}).Select(bson.M{"id": 1, "body": 1}).All(&result); err != nil {
		log.LogError("%v", err)
	}
	for _, v := range result {
		splits := strings.Split(v.Body, "/")
		emailUser := mongoData.getEmailUser(splits[1])
		for _, ve := range emailUser {
			log.LogTrace("<%s>\n", ve.Email)
			mongoBlob.updateMetadata(v.Id, ve.Email)
		}
	}
}

func (mongo *MongoDB) getEmailUser(username string) []data.User {
	users := []data.User{}
	if err := mongo.Users.Find(bson.M{"username": username}).Select(bson.M{"email": 1}).All(&users); err != nil {
		log.LogError("%s", err.Error())
	}
	return users
}

func (mongo *MongoBlob) updateMetadata(id, email string) {
	if err := mongo.Files.Update(bson.M{"_id": bson.ObjectIdHex(id)}, bson.M{"$addToSet": bson.M{"metadata.owners": email}}); err != nil {
		log.LogError("%s", err.Error())
	}
}
