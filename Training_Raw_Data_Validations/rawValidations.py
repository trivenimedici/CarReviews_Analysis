from logging import exception
from App_Logger import app_logger
import os
import json
import shutil
class RawValidations:
    def __init__(self,path):
        self.filepath=path
        self.schema_path="schema_training.json"
        self.logger=app_logger()
    

    def valuesFromDataSchema(self):
        """
            Method Name: valuesFromDataSchema
            Description: This method extracts all the relevant information from the pre-defined "Schema" file.
            Output:  column names, Number of Columns
            On Failure: Raise ValueError,KeyError,Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            with open(self.schema_path,'r') as f:
                dic=json.load(f)
                f.close()
            pattern=dic['samplefilename']
            no_of_columns = dic['NumberofColumns']
            col_names=dic['columnnames']
            file=open('Log_Files_Collection/schemaValidationsLogs.txt','a+')
            message="the file pattern is :: %s"+pattern+"\t"+"number of coumns are :: %s"%no_of_columns+"\t"+"the column names are :: %s "+col_names+"\n"
            self.logger.log(file,message)
            file.close()
        except ValueError:
            file=open('Log_Files_Collection/schemaValidationsLogs.txt','a+')
            self.logger.log(file,"ValueError:Value not found inside the schema_training.json")
            raise ValueError
        except KeyError:
            file=open('Log_Files_Collection/schemaValidationsLogs.txt','+a')
            self.logger.log(file,"KeyValueError:Key value error incorrect key passed")
            raise KeyError
        except exception as e:
            file=open('Log_Files_Collection/schemaValidationsLogs.txt','+a')
            self.logger.log(file,str(e))
            raise e
        return pattern,no_of_columns,col_names
                
    def regexFileCreation(self):
        """
            Method Name: regexFileCreation
            Description: This method is to create regex based on the file name given in schema file
                Output:  manual_regex_pattern
            On Failure: None
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        manual_regex_pattern="scrapper_data_[a-z0-9*]+_+[a-zA-Z\d{0-9}-]+.csv"
        return manual_regex_pattern
    
    def createDirForGoodBadData(self):
        """
            Method Name: createDirForGoodBadData
            Description: This method creates directories to store the Good Data and Bad Data after validating the training data.
            Output:  None
            On Failure: OsError
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            path=os.path.join('Training_Raw_files_validated/','Good_Raw/')
            if not os.path.isdir(path):
                os.makedirs(path)
            path=os.path.join('Training_Raw_files_validated/','Bad_Raw/')
            if not os.path.isdir(path):
                os.makedirs(path)
        except OSError as e:
            file=open('ErrorLogs.txt','a+')
            self.logger.log(file,"Error while creating directory %s:"% e)
            file.close()
            raise OSError
    
    def deleteExistingGoodDataTrainingFolder(self):
        """
            Method Name: deleteExistingGoodDataTrainingFolder
            Description: This method deletes the directory made  to store the Good Data after loading the data in the table. Once the good files are loaded in the DB,deleting the directory ensures space optimization.
            Output:  None
            On Failure: OsError
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            path='Training_Raw_files_validated/'
            if os.path.isdir(path+'Good_Raw/'):
                shutil.rmtree(path+'Good_Raw/')
                file=open('Log_Files_Collection/GeneralLog.txt','a+')
                self.logger.log(file,"Good Raw directory deleted successfully!!")
                file.close()
        except Exception as e:
            file=open("Log_Files_Collection/ErrorLogs.txt", 'a+')
            self.logger.log(file,"Error while deleting directory : %s" %e)
            file.close()
            raise e

    def deleteExistingBadTrainingDataFiles(self):
        """
            Method Name: deleteExistingBadTrainingDataFiles
            Description: This method deletes the directory made  to store the bad Data after loading the data in the table. Once the bad files are loaded in the DB,deleting the directory ensures space optimization.
            Output:  None
            On Failure: OsError
            Written By: triveni
            Version: 1.0
            Revisions: None
        """
        try:
            path='Training_Raw_files_validated/'
            if os.path.isdir(path+'Good_Raw/'):
                shutil.rmtree(path+'Good_Raw/')
                file=open('Log_Files_Collection/GeneralLog.txt','a+')
                self.logger.log(file,"Bad Raw directory deleted successfully!!")
                file.close()
        except OSError as e:
            file=open("Log_Files_Collection/ErrorLogs.txt", 'a+')
            self.logger.log(file,"Error while deleting directory : %s" %e)
            file.close()
            raise e


            


