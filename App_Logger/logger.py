from datetime import datetime
from genericpath import isdir
import os


class app_logger:
    def __init__(self) -> None:
        pass

    def log(self,fileObject,logMessage):
        self.now=datetime.now()
        self.current_date=self.now.date()
        self.current_time=self.now.strftime("%H:%M:%S")
        fileObject.write(str(self.current_date)+"_"+str(self.current_time)+"\t\t"+logMessage+"\n")

    def createLoggerFile(self,fileName):
        try:
            file_path="Log_Files_Collection/"+fileName
            if not os.path.exists(file_path):
                file= open(file_path,"a+")
            return file
        except OSError:
            file=open("Log_Files_Collection/ErrorLogs.txt","a+")
            self.log(file,"Error while creating log file for "+fileName+"%s:" % OSError)
            raise OSError

    def deleteExistingLogFiles(self):
        try:
            existing_files=[f for f in os.listdir("Log_Files_Collection")]
            for f in existing_files:
                os.remove("Log_Files_Collection/"+f)
        except OSError:
            raise OSError
