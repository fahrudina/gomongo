package main

import (
	"strings"

	"github.com/helioina/api/data"
	"github.com/helioina/api/log"
	"github.com/jmoiron/jsonq"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type MongoDB struct {
	Session     *mgo.Session
	Attachments *mgo.Collection
	Embeddeds   *mgo.Collection
	Users       *mgo.Collection
	Db          *mgo.Database
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
		Db:          session.DB("Smtpd"),
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
	//mongoBlob := CreateMongoBlob()
	//updateAttachment(mongoData, mongoBlob) //--> For Update Owners Attachment
	//updateEmbeded(mongoData, mongoBlob)    //--> For Update Owners Embeded

	//updateURLAttachment(mongoData) //--> For Update URL PATH Attachment
	//updateURLEmbeded(mongoData)    //--> For Update URL PATH Embeded

	updateAttachEmbedData(mongoData)

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

func updateURLAttachment(mongoData *MongoDB) {
	result := []Attachment{}
	if err := mongoData.Attachments.Find(bson.M{}).Select(bson.M{"id": 1, "urlpath": 1}).All(&result); err != nil {
		log.LogError("%v", err)
	}
	for _, v := range result {
		newUrl := strings.Replace(v.UrlPath, "https://edumail.id", "https://api.edumail.id", 1)
		mongoData.updateURL(v.Id, newUrl)
	}
}

func updateURLEmbeded(mongoData *MongoDB) {
	result := []Embedded{}
	if err := mongoData.Embeddeds.Find(bson.M{}).Select(bson.M{"id": 1, "urlpath": 1}).All(&result); err != nil {
		log.LogError("%v", err)
	}
	for _, v := range result {
		newUrl := strings.Replace(v.UrlPath, "https://edumail.id", "https://api.edumail.id", 1)
		mongoData.updateURL(v.Id, newUrl)
	}
}

func (mongo *MongoDB) getEmailUser(username string) []data.User {
	users := []data.User{}
	if err := mongo.Users.Find(bson.M{"username": username}).Select(bson.M{"email": 1}).All(&users); err != nil {
		log.LogError("%s", err.Error())
	}
	return users
}

func (mongo *MongoDB) updateURL( /*id,*/ attachmentId, attachmentUrl string) {
	cols, err := mongo.GetCollectionNames()
	if err != nil {
		log.LogError("%s", err.Error())
	}

	for _, v := range cols {
		if strings.HasPrefix(v["name"].(string), "Messages") {
			col := mongo.Session.DB("Smtpd").C(v["name"].(string))
			if err := col.Update(bson.M{ /*"id": id,*/ "attachments.id": attachmentId},
				bson.M{"$set": bson.M{"attachments.$.urlpath": attachmentUrl}}); err != nil {
				log.LogError("ID : %s URL: %s -- %s", attachmentId, attachmentUrl, err.Error())
			} else {
				log.LogInfo("Success Update ID %s -- URL %s ", attachmentId, attachmentUrl)
			}
		}
	}
}

func updateAttachEmbedData(mongo *MongoDB) {
	cols, err := mongo.GetCollectionNames()
	if err != nil {
		log.LogError("%s", err.Error())
	}

	for _, v := range cols {
		result := []data.Message{}
		resulte := []data.Message{}
		if strings.HasPrefix(v["name"].(string), "Messages") {
			col := mongo.Session.DB("Smtpd").C(v["name"].(string))
			if err := col.Find(bson.M{"$where": "this.attachments.length > 0"}).Select(bson.M{"id": 1, "attachments.id": 1, "attachments.urlpath": 1}).All(&result); err != nil {
				log.LogError("%v", err)
			}
			for _, v := range result {
				for _, va := range v.Attachments {
					newUrl := strings.Replace(va.UrlPath, "https://edumail.id", "https://api.edumail.id", 1)
					if err := col.Update(bson.M{"id": v.Id, "attachments.id": va.Id},
						bson.M{"$set": bson.M{"attachments.$.urlpath": newUrl}}); err != nil {
						log.LogError("ID : %s URL: %s -- %s", va.Id, newUrl, err.Error())
					} else {
						log.LogInfo("Success Update ID %s -- URL %s ", va.Id, newUrl)
					}
				}
			}

			if err := col.Find(bson.M{"$where": "this.embeddeds.length > 0"}).Select(bson.M{"id": 1, "embeddeds.id": 1, "embeddeds.urlpath": 1}).All(&resulte); err != nil {
				log.LogError("%v", err)
			}
			for _, v := range resulte {
				for _, ve := range v.Embeddeds {
					newUrl := strings.Replace(ve.UrlPath, "https://edumail.id", "https://api.edumail.id", 1)
					if err := col.Update(bson.M{"id": v.Id, "embeddeds.id": ve.Id},
						bson.M{"$set": bson.M{"embeddeds.$.urlpath": newUrl}}); err != nil {
						log.LogError("ID : %s URL: %s -- %s", ve.Id, newUrl, err.Error())
					} else {
						log.LogInfo("Success Update ID %s -- URL %s ", ve.Id, newUrl)
					}
				}
			}

		}
	}
}

func (mongo *MongoBlob) updateMetadata(id, email string) {
	if err := mongo.Files.Update(bson.M{"_id": bson.ObjectIdHex(id)}, bson.M{"$addToSet": bson.M{"metadata.owners": email}}); err != nil {
		log.LogError("%s", err.Error())
	}
}

func (mongo *MongoDB) GetCollectionNames() ([]map[string]interface{}, error) {
	raw := make(map[string]interface{})
	err := mongo.Db.Run(bson.D{{"listCollections", 1}}, &raw)
	if err != nil {
		return nil, err
	}
	jq := jsonq.NewQuery(raw)
	items, err := jq.ArrayOfObjects("cursor", "firstBatch")
	if err != nil {
		return nil, err
	}

	return items, nil
}
