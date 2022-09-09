import csv
from App_Logger import logger
import pymongo
import csv
import json
import pandas as pd
class DBconnectionToApp:
    global log_file,error_file
    log_file=open("Log_Files_Collection/Webscrapping_app_logs.txt","a+") 
    error_file =open("ErrorLogs.txt","a+") 
    def __init__(self,username,password):
        try:
            self.username=username
            self.password=password
            self.url="mongodb+srv://{}:{}@cluster0.nlf9qxq.mongodb.net/CarDekhoWebScrapping?retryWrites=true&w=majority".format(self.username, self.password)
        except Exception as e:
            logger.app_logger().log(error_file,"Something went wrong while initation process %s:" % e)
            raise Exception(f"(__init__): Something went wrong on initiation process\n" + str(e))

    def getMongoDBClientObject(self):
        try:
            mongo_client = pymongo.MongoClient(self.url)
            logger.app_logger().log(log_file,'Got Mongo client object created')
            return mongo_client
        except Exception as e:
            logger.app_logger().log(error_file,"Something went wrong while creating client object %s:" % e)
            raise Exception("(getMongoDBClientObject): Something went wrong on creation of client object\n" + str(e))        
    
    def closeMongoDBConnection(self,mongo_client):
         try:
            mongo_client.close()
            logger.app_logger().log(log_file,'Closing Mongo connection')
         except Exception as e:
            logger.app_logger().log(error_file,"Something went wrong on closing connection %s:" % e)
            raise Exception(f"Something went wrong on closing connection\n" + str(e))

    def isDatabasePresent(self,db_name):
        try:
            mongo_client=self.getMongoDBClientObject()
            if db_name in mongo_client.list_database_names():
                logger.app_logger().log(log_file,'Database is present')
                return True
            else:
              #  mongo_client.close()
                return False
        except Exception as e:
            logger.app_logger().log(error_file,"Failed on checking if the database is present or not %s:" % e)
            raise Exception("(isDatabasePresent): Failed on checking if the database is present or not \n" + str(e))

    def createDatabase(self, db_name):
        try:
            database_check_status = self.isDatabasePresent(db_name=db_name)
            if not database_check_status:
                mongo_client = self.getMongoDBClientObject()
                database = mongo_client[db_name]
                logger.app_logger().log(log_file,'Database is created successfully!!')
                return database
            else:
                mongo_client = self.getMongoDBClientObject()
                database = mongo_client[db_name]
                logger.app_logger().log(log_file,'Database is created successfully!!')
                return database
        except Exception as e:
            logger.app_logger().log(error_file,"Failed on creating database %s:" % e)
            raise Exception(f"(createDatabase): Failed on creating database\n" + str(e))



    def dropDatabase(self, db_name):
        try:
            mongo_client = self.getMongoDBClientObject()
            if db_name in mongo_client.list_database_names():
                mongo_client.drop_database(db_name)
                logger.app_logger().log(log_file,'Database is dropped successfully!!')
                return True
        except Exception as e:
            logger.app_logger().log(error_file,"Failed on deleting database %s:" % e)
            raise Exception(f"(dropDatabase): Failed to delete database {db_name}\n" + str(e))
   
    def getDatabase(self, db_name):
        try:
            mongo_client = self.getMongoDBClientObject()
            #mongo_client.close()
            logger.app_logger().log(log_file,'Got Database successfully!!')
            return mongo_client[db_name]
        except Exception as e:
            logger.app_logger().log(error_file,"Failed to get the database list %s:" % e)
            raise Exception(f"(getDatabase): Failed to get the database list")
    
    def getCollection(self, collection_name, db_name):
        try:
            database = self.getDatabase(db_name)
            logger.app_logger().log(log_file,'Got database collection successfully!!')
            return database[collection_name]
        except Exception as e:
            logger.app_logger().log(error_file,"Failed to get the database Collection %s:" % e)
            raise Exception(f"(getCollection): Failed to get the database Collection.")
    
    def isCollectionPresent(self, collection_name, db_name):
        try:
            database_status = self.isDatabasePresent(db_name=db_name)
            if database_status:
                database = self.getDatabase(db_name=db_name)
                if collection_name in database.list_collection_names():
                    logger.app_logger().log(log_file,'collection is present in mongo')
                    return True
                else:
                    logger.app_logger().log(log_file,'Collection is not present in  Mongo')
                    return False
            else:
                return False
        except Exception as e:
            logger.app_logger().log(error_file,"Failed on check  database collection %s:" % e)
            raise Exception(f"(isCollectionPresent): Failed to check collection\n" + str(e))

    def createCollection(self, collection_name, db_name):
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if not collection_check_status:
                database = self.getDatabase(db_name=db_name)
                collection = database[collection_name]
                logger.app_logger().log(log_file,' Created mongo collection')
                return collection
        except Exception as e:
            logger.app_logger().log(error_file,"Failed on create collection %s:" % e)
            raise Exception(f"(createCollection): Failed to create collection {collection_name}\n" + str(e))

    def dropCollection(self, collection_name, db_name):
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                collection.drop()
                logger.app_logger().log(log_file,'dropped Mongo collection')
                return True
            else:
                logger.app_logger().log(log_file,'dropped Mongo collection')
                return False
        except Exception as e:
            logger.app_logger().log(error_file,"Failed to drop database collection %s:" % e)
            raise Exception(f"(dropCollection): Failed to drop collection {collection_name}")

    def insertRecordFromCSVFile(self,db_name,collection_name,csv_file,header):
        try:
            collection = self.getCollection(collection_name=collection_name, db_name=db_name)
            csvfile = open(csv_file, 'r')
            reader = csv.DictReader( csvfile )
            for each in reader:
                row={}
                for field in header:
                    row[field]=each[field]
                print(row)
                collection.insert_one(row)
                logger.app_logger().log(log_file,'inserted records from csv to Mongo connection')
            return f"rows inserted "
        except Exception as e:
            logger.app_logger().log(error_file,"Something went wrong while inserting record from csv file %s:" % e)
            raise Exception(f"(insertRecord): Something went wrong on inserting record from csv\n" + str(e))
 

    def insertRecord(self, db_name, collection_name, record):
        try:
            collection = self.getCollection(collection_name=collection_name, db_name=db_name)
            collection.insert_one(record)
            sum = 0
            logger.app_logger().log(log_file,'successfully inserted record to mongo')
            return f"rows inserted "
        except Exception as e:
            logger.app_logger().log(error_file,"Something went wrong while insertinf record %s:" % e)
            raise Exception(f"(insertRecord): Something went wrong on inserting record\n" + str(e))

    def insertRecords(self, db_name, collection_name, records):
        try:
            collection = self.getCollection(collection_name=collection_name, db_name=db_name)
            record = list(records.values())
            collection.insert_many(record)
            sum = 0
            logger.app_logger().log(log_file,'successfully inserted records to mongo')
            return f"rows inserted "
        except Exception as e:
            logger.app_logger().log(error_file,"Something went wrong while inserting records %s:" % e)
            raise Exception(f"(insertRecords): Something went wrong on inserting record\n" + str(e))

    def findfirstRecord(self, db_name, collection_name,query=None):
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            print(collection_check_status)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                print(collection)
                firstRecord = collection.find_one(query)
                logger.app_logger().log(log_file,'successfully found record to mongo')
                return firstRecord
        except Exception as e:
            logger.app_logger().log(error_file,"Failed to find record for the given collection and  database %s:" % e)
            raise Exception(f"(findRecord): Failed to find record for the given collection and database\n" + str(e))

    def findAllRecords(self, db_name, collection_name):
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                findAllRecords = collection.find()
                logger.app_logger().log(log_file,'successfully found records to mongo')
                return findAllRecords
        except Exception as e:
            logger.app_logger().log(error_file,"Failed to find record for the given collection %s:" % e)
            raise Exception(f"(findAllRecords): Failed to find record for the given collection and database\n" + str(e))

    def findRecordOnQuery(self, db_name, collection_name, query):
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                findRecords = collection.find(query)
                logger.app_logger().log(log_file,'successfully found record to mongo based on query')
                return findRecords
        except Exception as e:
            logger.app_logger().log(error_file,"Failed to find record in db %s:" % e)
            raise Exception(
                f"(findRecordOnQuery): Failed to find record for given query,collection or database\n" + str(e))

    def updateOneRecord(self, db_name, collection_name, query):
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                previous_records = self.findAllRecords(db_name=db_name, collection_name=collection_name)
                new_records = query
                updated_record = collection.update_one(previous_records, new_records)
                logger.app_logger().log(log_file,'Updated record successfully')
                return updated_record
        except Exception as e:
            logger.app_logger().log(error_file,"Failed to update record in  database %s:" % e)
            raise Exception(
                f"(updateRecord): Failed to update the records with given collection query or database name.\n" + str(
                    e))

    def updateMultipleRecord(self, db_name, collection_name, query):
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                previous_records = self.findAllRecords(db_name=db_name, collection_name=collection_name)
                new_records = query
                updated_records = collection.update_many(previous_records, new_records)
                logger.app_logger().log(log_file,'Updated records successfully')
                return updated_records
        except Exception as e:
            logger.app_logger().log(error_file,"Failed to update multiple records in database %s:" % e)
            raise Exception(
                f"(updateMultipleRecord): Failed to update the records with given collection query or database name.\n" + str(
                    e))

    def deleteRecord(self, db_name, collection_name, query):
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                collection.delete_one(query)
                logger.app_logger().log(log_file,'deleted record successfully')
                return "1 row deleted"
        except Exception as e:
            logger.app_logger().log(error_file,"Failed to delete records with given collection query or database name %s:" % e)
            raise Exception(
                f"(deleteRecord): Failed to delete the records with given collection query or database name.\n" + str(
                    e))

    def deleteRecords(self, db_name, collection_name, query):
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            if collection_check_status:
                collection = self.getCollection(collection_name=collection_name, db_name=db_name)
                collection.delete_many(query)
                logger.app_logger().log(log_file,'deleted records successfully')
                return "Multiple rows deleted"
        except Exception as e:
            logger.app_logger().log(error_file,"failed to delete records with given collection query or database name %s:" % e)
            raise Exception(
                f"(deleteRecords): Failed to delete the records with given collection query or database name.\n" + str(
                    e))

    def getDataFrameOfCollection(self, db_name, collection_name):
        try:
            all_Records = self.findAllRecords(collection_name=collection_name, db_name=db_name)
            dataframe = pd.DataFrame(all_Records)
            logger.app_logger().log(log_file,'got data frame from collection successfully')
            return dataframe
        except Exception as e:
            logger.app_logger().log(error_file,"Failed to get dataframe collection from  database %s:" % e)
            raise Exception(
                f"(getDataFrameOfCollection): Failed to get DatFrame from provided collection and database.\n" + str(e))

    def saveDataFrameIntoCollection(self, collection_name, db_name, dataframe):
        try:
            collection_check_status = self.isCollectionPresent(collection_name=collection_name, db_name=db_name)
            dataframe_dict = json.loads(dataframe.T.to_json())
            if collection_check_status:
                self.insertRecords(collection_name=collection_name, db_name=db_name, records=dataframe_dict)
                logger.app_logger().log(log_file,'got data saved into collection successfully')
                return "Inserted"
            else:
                self.createDatabase(db_name=db_name)
                self.createCollection(collection_name=collection_name, db_name=db_name)
                self.insertRecords(db_name=db_name, collection_name=collection_name, records=dataframe_dict)
                logger.app_logger().log(log_file,'got data saved into collection successfully')
                return "Inserted"
        except Exception as e:
            logger.app_logger().log(error_file,"Failed to save dataframe value into collection %s:" % e)
            raise Exception(
                f"(saveDataFrameIntoCollection): Failed to save dataframe value into collection.\n" + str(e))

    def getResultToDisplayOnBrowser(self, db_name, collection_name):
        try:
            response = self.findAllRecords(db_name=db_name, collection_name=collection_name)
            result = [i for i in response]
            logger.app_logger().log(log_file,'got result displayed on browser')
            return result
        except Exception as e:
            logger.app_logger().log(error_file,"something went wrong on getting result from  database%s:" % e)
            raise Exception(
                f"(getResultToDisplayOnBrowser) - Something went wrong on getting result from database.\n" + str(e))