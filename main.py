import pandas as pd
import os
from flask import Flask, request, render_template,redirect,url_for
from flask_cors import CORS,cross_origin
import flask_monitoringdashboard as dashboard
from wsgiref import simple_server
import os
from App_Logger import logger
from WebScrapping_App import reviews
from WebScrapping_App import webapp_dbconnection

os.putenv("LANG","en_US.UTF-8")
os.putenv("LC_ALL","en_US.UTF-8")

app=Flask(__name__)
dashboard.bind(app)
CORS(app)



@app.route("/",methods=['GET','POST'])
@cross_origin()
def webscrapping_app_home(): 
    try:
        filetoopen = open("Log_Files_Collection/Webscrapping_app_logs.txt","w",encoding='utf-8')
        logger.app_logger().log(filetoopen,"Web Scrapping app started")
        if request.method=="POST":
            searchString = request.form['content']
            if " " not in searchString:
                searchresult = "Unable to find the searched car"
            else:    
                searchString =searchString.replace(" ","/",1).replace(" ","-",2).lower()
                global car_name
                car_name =searchString.replace("/","_")
                logger.app_logger().log(filetoopen,f'the search string inserted by the user is {searchString}')
                searchresult = reviews.GetReviews().navigatetoApp(searchString)
            if(searchresult == "Unable to find the searched car"):
                return render_template('tryagain.html')
            else:
                mongoClient = webapp_dbconnection.DBconnectionToApp(username='mongotest', password='mongo123')
                if mongoClient.isCollectionPresent(collection_name=car_name, db_name="CarDekhoWebScrapping"):
                    logger.app_logger().log(filetoopen,'Mongo collection is present')
                    response = mongoClient.findAllRecords(db_name="CarDekhoWebScrapping", collection_name=car_name)
                    review= [i for i in response]
                    logger.app_logger().log(filetoopen,'review dta ais '+str(review))
                    result = [review[i] for i in range(0, len(review))]
                    reviews.GetReviews().saveDataFrameDatatoFile("DataSet_Files_Collection/scrapper_data_"+car_name+".csv",pd.DataFrame(result))
                    logger.app_logger().log(filetoopen,'Data saved in csv file successully')
                    return render_template('reviews.html',rows=review)
                else:
                    logger.app_logger().log(filetoopen,'collection not found so getting details')
                    global collection_name
                    collection_name = reviews.GetReviews().getReviewsToDisplay(car_name,'mongotest','mongo123')
                    logger.app_logger().log(filetoopen,'the collection name is '+str(collection_name))
                    return redirect(url_for('feedback'))
        else:
            return render_template('index.html')
    except Exception as e:
        file=open("ErrorLogs.txt","a+",encoding='utf-8')
        logger.app_logger().log(file,"Error in the Web Scrapping app Home function %s:" % e)
        raise e

@app.route('/feedback', methods=['GET'])
@cross_origin()
def feedback():
    global log_file,error_file
    log_file=open("Log_Files_Collection/Webscrapping_app_logs.txt","a+",encoding='utf-8')
    error_file =open("ErrorLogs.txt","a+") 
    try:
        global collection_name
        if collection_name is not None:
            logger.app_logger().log(log_file,'in the if condition and collection name is not None')
            mongoClient = webapp_dbconnection.DBconnectionToApp(username='mongotest', password='mongo123')
            rows = mongoClient.findAllRecords(db_name="CarDekhoWebScrapping", collection_name=collection_name)
            logger.app_logger().log(log_file,'the rows are '+str(rows))
            review = [i for i in rows]
            logger.app_logger().log(log_file,'the review values are '+str(review))
            dataframe = pd.DataFrame(review)
            reviews.GetReviews().saveDataFrameDatatoFile(file_name="DataSet_Files_Collection/scrapper_data_"+car_name+".csv", dataframe=pd.DataFrame(dataframe))
            collection_name = None
            return render_template('reviews.html', rows=review)
        else:
            logger.app_logger().log(log_file,'in the else loop and collection name is None')
            return render_template('tryagain.html')
    except Exception as e:
        file=open("ErrorLogs.txt","a+",encoding='utf-8')
        logger.app_logger().log(file,"Something went wrong on retrieving feedback %s:" % e)
        raise Exception("(feedback) - Something went wrong on retrieving feedback.\n" + str(e))
           

port= int(os.getenv("PORT",5000))
if __name__=="__main__":
    host="0.0.0.0"
    httpd=simple_server.make_server(host,port,app)
    httpd.serve_forever()