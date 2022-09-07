from ast import Raise
import os
from flask import Flask, request, render_template,Response
from flask_cors import CORS,cross_origin
import flask_monitoringdashboard as dashboard
from wsgiref import simple_server
import json
from os import listdir
import os
from App_Logger import logger
from WebScrapping_App import reviews,webapp_dbconnection

os.putenv("LANG","en_US.UTF-8")
os.putenv("LC_ALL","en_US.UTF-8")

app=Flask(__name__)
dashboard.bind(app)
CORS(app)


@app.route("/",methods=['GET','POST'])
@cross_origin()
def webscrapping_app_home():
    logger.app_logger().deleteExistingLogFiles()
    filetoopen =logger.app_logger().createLoggerFile("Webscrapping_app_logs.txt")
    try:
        logger.app_logger().log(filetoopen,"Web Scrapping app started")
        if request.method=="POST":
            searchString = request.form['content'].replace(" ","-")
            logger.app_logger().log(f'the search string inserted by the user is {searchString}')
        else:
            return render_template('index.html')
    except Exception as e:
        file=open("Log_Files_Collection/ErrorLogs.txt","a+")
        logger.app_logger().log(file,"Error in the Web Scrapping app Home function %s:" % e)
        raise e


port= int(os.getenv("PORT",5000))
if __name__=="__main__":
    host="0.0.0.0"
    httpd=simple_server.make_server(host,port,app)
    httpd.serve_forever()