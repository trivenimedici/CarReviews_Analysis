
from datetime import datetime
from locale import strxfrm
import os
import json
from posixpath import split
import shutil
from time import strftime
from App_Logger import logger
from datetime import datetime
import re
import csv
import pandas as pd
class RawDataSetValidations:
    """
    name:RawDataSetValidations
    description: this class to validate the input data set files to identify good and bad data from the incoming files
    written by: triveni
    version:1.0
    """
    def __init__(self,path):
        self.Batch_data_files_path=path
        self.schemaFile="Training_Data_Schema.json"
        self.log=logger.app_logger
    def getValuesFromSchemaFile(self):
        """
        name:getValuesFromSchemaFile
        description: this method is for getting the schema file values 
        output: file pattern name,no of columns, column names
        written by: triveni
        version:1.0
        """
        try:
            with open(self.schemaFile,'r') as f:
                dic = json.load(f)
                InputDataFilePattern = dic["trainingdataFilePattern"]
                no_of_Columns= dic["numberOfColumns"]
                column_names= dic["columnNames"]
                file=open('Log_Files_Collection/valuesfromSchemaValidationLog.txt','a+')
                message="Input filepattern to validate is : %s"%InputDataFilePattern+"\t"+"Number of Columns are :%s"%no_of_Columns+"\t"+"Name of columns are :%s"%column_names+"\n"
                self.log().log(file,message)
                file.close()
        except ValueError:
            f=open('Log_Files_Collection/valuesfromSchemaValidationLog.txt','a+')
            self.log().log(f,"ValueError:Value not found inside Training_Data_Schema.json")
            f.close()
            raise ValueError
        except KeyError:
            f= open('Log_Files_Collection/valuesfromSchemaValidationLog.txt','a+')
            self.log().log(f,"KeyError:Key not found inside the Training_Data_Schema.json")
            f.close()
            raise KeyError
        except Exception as e:
            f=open('ErrorLogs.txt')
            self.log().log(f,"Got the exception while reading the data from  Training_Data_Schema.json %s"%e)
            f.close()
            raise e
        return InputDataFilePattern, no_of_Columns,column_names

    def regexPatterntoValidateFile(self):
        """
        name:regexPatterntoValidateFile
        description: this method is to return the file pattern
        output: regexfile pattern
        written by: triveni
        version:1.0
        """
        inputRegexFilePattern="scrapper_data_+[a-z]+_[0-9-a-z]+.csv"
        return inputRegexFilePattern

    def createDirForGoodBadFiles(self):
        """
        name:createDirForGoodBadFiles
        description: this method is to create directory good and bad files directory
        output: None
        written by: triveni
        version:1.0
        """
        try:
            path=os.path.join('Training_Data_Validated_Files/','Good_Raw/')
            if not os.path.isdir(path):
                os.makedirs(path)
                file=open('Log_Files_Collection/Training_Model_Logs.txt','a+')
                self.log().log(file,"Training data good files directory created successfully!!")
                file.close()
            path =os.path.join('Training_Data_Validated_Files/','Bad_Raw/')
            if not os.path.isdir(path):
                os.makedirs(path)
                file=open('Log_Files_Collection/Training_Model_Logs.txt','a+')
                self.log().log(file,"Training data bad files created successfully!!")
                file.close()
        except OSError as e:
            file =open('ErrorLogs.txt','a+')
            self.log().log(file,'Error while creating Directory %s:'%e)
            file.close()
            raise OSError
    def deleteExistingGoodDataTrainingRawFiles(self):
        """
        name:deleteExistingGoodTrainingFolder
        description: this method is used for deleting the good training data
        output: None
        written by: triveni
        version:1.0
        """
        try:
            path=os.path.join('Training_Data_Validated_Files/','Good_Raw/')
            if os.path.isdir(path):
                shutil.rmtree(path)
                file=open('Log_Files_Collection/Training_Model_Logs.txt','a+')
                self.log().log(file,"Training data good files deleted successfully!!")
                file.close()
        except OSError:
            f=open('ErrorLogs.txt','a+')
            self.log().log(f,'Error while deleting the training good raw data files')
            f.close()
     
    def deleteExistingBadDataTrainingRawFiles(self):
        """
        name:deleteExistingBadDataTrainingRawFiles
        description: this method is used for deleting the bad training data raw files directory
        output: None
        written by: triveni
        version:1.0
        """
        try:
            path=os.path.join('Training_Data_Validated_Files/','Bad_Raw/')
            if os.path.isdir(path):
                shutil.rmtree(path)
                file=open('Log_Files_Collection/Training_Model_Logs.txt','a+')
                self.log().log(file,"Training data Bad files deleted successfully!!")
                file.close()
        except OSError:
            f=open('ErrorLogs.txt','a+')
            self.log().log(f,'Error while deleting the training bad raw data files')
            f.close()
    def movBadFilestoArchive(self):
        """
        name:movBadFilestoArchive
        description: this method is used for move the bad raw data files to archieve
        output: None
        written by: triveni
        version:1.0
        """
        current_datetime=datetime.now
        current_date=current_datetime.date()
        current_time=strftime('%H%M%S')
        try:
            logfile=open('Log_Files_Collection/Training_Model_Logs.txt','a+')
            source='Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path="TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest='TrainingArchiveBadData/BadData_' + str(current_date)+"_"+str(current_time)
                if os.path.isdiir(dest):
                    os.makedirs(dest)
                files=os.listdir(source)
                for f in files: 
                    if f not in os.listdir(dest):
                        shutil.move(source+f,dest)
                        self.log().log(logfile,"Moved the file %s"%source+f+" to the folder %s"%dest)
                self.log().log(logfile,'All Bad Files moved to the Archive')
                if os.path.isdir(os.path.join('Training_Raw_files_validated/','Good_Raw/')):
                    shutil.rmtree(os.path.join('Training_Raw_files_validated/','Good_Raw/'))
                    self.log.log(logfile,"Bad Raw Data Folder Deleted successfully!!")
                logfile.close()
        except Exception as e:
            file = open("ErrorLogs.txt", 'a+')
            self.log.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e
    def validateFileNameRawTrainingData(self):
        """
        name:validateFileNameRawTrainingData
        description: this method is for valdiating the file name pattern for the input training dataset
        output: None
        written by: triveni
        version:1.0
        """
        try:
            self.deleteExistingBadDataTrainingRawFiles()
            self.deleteExistingGoodDataTrainingRawFiles()
            self.createDirForGoodBadFiles()
            filesinDir=[(f for f in os.listdir(self.Batch_data_files_path))]
            expectedpattern=self.regexPatterntoValidateFile()
            f=open('Log_Files_Collection/Training_Model_Logs.txt','a+')
            for filename in filesinDir:
                if(re.match(expectedpattern,filename)):
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw")
                    self.log().log(f,"Valid File name!! File moved to Good Raw Folder :: %s" % filename)
                else: 
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    self.log().log(f,"Valid File name!! File moved to Bad Raw Folder :: %s" % filename)
            f.close()
        except Exception as e:
            f = open("ErrorLogs.txt", 'a+')
            self.log().log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e
    def validateColumnLengthforTrainingDataFile(self,noOfColumns):
        """
        name:validateColumnLengthforTrainingDataFile
        description: this method is for valdiating number of columns for the input training dataset
        output: None
        written by: triveni
        version:1.0
        """
        try:
            logfile=open('Log_Files_Collection/Training_Model_Logs.txt','a+')
            self.log().log(logfile,"Validation for number of columns started")
            for file in os.listdir("Training_Raw_files_validated/Good_Raw/"):
                csv=pd.read_csv("Training_Raw_files_validated/Good_Raw/"+file)
                if csv.shape[1]==noOfColumns:
                    pass
                else:
                    shutil.copy("Training_Batch_Files/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.log().log(logfile,"Valid File name!! File moved to Good Raw Folder :: %s" % file)
            self.log().log(logfile,"Validation for number of columns completed")
            logfile.close()
        except OSError:
            f = open("ErrorLogs.txt", 'a+')
            self.log.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("ErrorLogs.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
    def validateMissingDatainWholeColumn(self):
        """
        name:validateMissingDatainWholeColumn
        description: this method is for valdiating missing column values for the input training dataset in good raw data folder
        output: None
        written by: triveni
        version:1.0
        """
        try:
            logfile=open('Log_Files_Collection/Training_Model_Logs.txt','a+')
            self.log().log(logfile,"Validation for missing data in columns started")
            for file in os.listdir("Training_Raw_files_validated/Good_Raw/"):
                csv=pd.read_csv("Training_Raw_files_validated/Good_Raw/"+file)
                count=0
                for columns in csv:
                    if (len(csv[columns]) -csv[columns].count())==len(csv[columns]):
                        count+=1
                        shutil.move("Training_Raw_files_validated/Good_Raw/" + file,
                                    "Training_Raw_files_validated/Bad_Raw")
                        self.log.log(logfile,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    csv.rename(columns={"Unnamed: 0": "CarReviews"}, inplace=True)
                    csv.to_csv("Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)
            logfile.close()
        except OSError:
            f = open("ErrorLogs.txt", 'a+')
            self.log.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("ErrorLogs.txt", 'a+')
            self.log.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e














            

            


        