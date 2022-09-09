from datetime import datetime
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
            file=open("ErrorLogs.txt","a+")
            self.log(file,"Error while creating log file for "+fileName+"%s:" % OSError)
            raise OSError

    def deleteExistingLogFiles(self):
        try:
            existing_files="Log_Files_Collection"
            for f in os.listdir(existing_files):
                file =os.path.join(existing_files,f)
                if os.path.isfile(file):
                    print('Deleting file:', file)
                    os.remove(file)
        except OSError:
            file=open("ErrorLogs.txt","a+")
            self.log(file,"Error while deleteing log file for %s:" % OSError)
            raise OSError
