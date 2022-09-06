from datetime import datetime
import os

class app_logger:
    def __init__(self) -> None:
        pass

    def log(self,fileObject,logMessage):
        self.now=datetime.now()
        self.current_date=self.now.date()
        self.current_time=self.now.strftime("%H:%M:%S")
        fileObject.write(self.current_date+"_"+self.current_time+"\t\t"+logMessage+"\n")

    def createLoggerFile(self,fileName):
        try:
            path=os.path.join("Log_Files_Collection/",fileName)
            if not os.path.isdir(path):
                os.makedirs(path)
            return path
        except OSError:
            file=open("Log_Files_Collection/GeneralLog.txt","a+")
            self.log(file,"Error while creating log file for "+fileName+"%s:" % OSError)
            return OSError

    def deleteExistingLogFileinDirectory(self,fileName):
        try:
            path =os.path.join("Log_Files_Collection/",fileName)
            if os.path.isdir(path):
                os.removedirs(fileName)
        except OSError:
            file=open("Log_Files_Collection/GeneralLog.txt","a+")
            self.log(file,"Error while deleting the file"+fileName+"%s:" % OSError)
            return OSError
