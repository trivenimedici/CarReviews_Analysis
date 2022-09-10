from urllib.request import Request, urlopen
from App_Logger import logger
from flask import Flask, Request
from urllib.request import Request,urlopen
from bs4 import BeautifulSoup as bs
import numpy as np
import pandas as pd
from WebScrapping_App import webapp_dbconnection as dbconnection
import os
class GetReviews:
    def __init__(self) -> None:
         pass
    global log_file,error_file,productname
    log_file=open("Log_Files_Collection/Webscrapping_app_logs.txt","w",encoding='utf-8')
    error_file =open("ErrorLogs.txt","a+",encoding='utf-8') 
    def navigatetoApp(self,searchString):
        try:
            app_url="https://www.cardekho.com/"+searchString+"/user-reviews"
            logger.app_logger().log(log_file,'the application url is '+app_url)
            req=Request(app_url,headers={'User-Agent':'Mozilla/5.0'})
            resp=urlopen(req)
            logger.app_logger().log(log_file,'opened application url')
            global reviewpage
            reviewpage= resp.read()
            if "ratingvalue" in str(reviewpage):
                productname = searchString.replace("/"," ")
                logger.app_logger().log(log_file,'navigated to the application url '+app_url+' Able to find the car ' + productname)
                return  "Able to find the searched car"
            else:
                logger.app_logger().log(log_file,'navigated to the application url '+app_url+' But unable to find the product')
                return  "Unable to find the searched car"
        except Exception as e:
            logger.app_logger().log(error_file,"Something went wrong while opening app url %s:" % e)
            raise e


        
    def get_reviews_from_ui(self):
        try:
            print(reviewpage)
            logger.app_logger().log(log_file,f'the review page details are {reviewpage}')
            reviewpage_html=bs(reviewpage)
            product_name =(reviewpage_html.text).split("Reviews - ")[0]
            review_value=reviewpage_html.find('span',{'class':'ratingvalue'}).text
            reviews_title =reviewpage_html.findAll('div',{'class':'contentspace'})
            titles,review_desc,review_authorname,reviewer_name,user_rating,review_date,review_datelist=([] for i in range(7))
            for i in range(0,len(reviews_title)):
                    titles.append(str(reviews_title[i].h3.a.text))
            review_content =  reviewpage_html.findAll('p',{'class':'contentheight'})
            for i in range(0,len(review_content)):
                    review_desc.append(str(review_content[i].span.text))
            review_author =  reviewpage_html.findAll('div',{'class':'authorSummary'})
            for i in range(0,len(review_author)):
                    review_authorname.append(review_author[i].findAll('div',{'class':'name'}))
            review_name=[]
            review_name=[ele for ele in review_authorname if ele != []]
            for i in range(0,len(review_name)):
                    reviewer_name.append((review_name[i][0].text).split('By ')[1])
            car_price =  str(reviewpage_html.findAll('div',{'class':'price'})[0].span.text).replace('*Get On Road Price','')
            title = str(reviewpage_html.findAll('div',{'class':'title'})[0].a.text)
            user_all_ratings = reviewpage_html.findAll('div',{'class':'starRating'})
            for  i in range(0,len(user_all_ratings)):
                    user_rating.append(len(user_all_ratings[i].findAll('span',{'class':'icon-star-full-fill'}))+int(len(user_all_ratings[i].findAll('span',{'class':'icon-star-half-empty'})))/2)
            resp_list=[]
            for i in range(0,len(review_author)):
                    resp_list.append(review_author[i].findAll('div',{'class':'date'}))
            review_datelist=[ele for ele in resp_list if ele != []]
            for i in range(0,len(review_datelist)):
                    temp = str(review_datelist[i][0].text).replace("On: ","").split(" ")
                    review_date.append(temp[0]+' '+ temp[1]+' '+ temp[2])
            rating_dict = dict(Car_Name= np.array(product_name,dtype=object), OverAll_Rating= np.array(review_value,dtype=object), Price= np.array(car_price,dtype=object), Rating_Title=np.array(titles,dtype=object),
                                Reviews_Description=np.array(review_desc,dtype=object), Review_Author= np.array(reviewer_name,dtype=object), User_Rating= np.array(user_rating,dtype=object), Review_Date= np.array(review_date,dtype=object) )
            logger.app_logger().log(log_file,f'All the rating dictory values are {rating_dict} ')           
            df = pd.DataFrame.from_dict(rating_dict) 
            logger.app_logger().log(log_file,'Converted to the dataframe')
            return df
        except Exception as e:
            logger.app_logger().log(error_file,"Something went wrong while geting review data %s:" % e)
            raise e
    

    def saveDataFrameDatatoFile(self,file_name,dataframe):
        try:
            dataframe.to_csv(file_name)
            logger.app_logger().log(log_file,'Converted to the csv')
        except Exception as e:
            logger.app_logger().log(error_file,"Unable to save date to the csv file %s:" % e)
            raise Exception(f"(saveDataframetofile)  - Unable to save data to the file.\n" + str(e))

    def getReviewsToDisplay( self,searchstring, username, password):
        try:
            mongoClient = dbconnection.DBconnectionToApp(username, password)
            db_search = mongoClient.findfirstRecord(db_name = "CarDekhoWebScrapping",collection_name=searchstring,query="{'product_name': searchstring}")
            if db_search is not None:
                logger.app_logger().log(log_file,"Yes Present "+str(len(db_search)))
            else:
                logger.app_logger().log(log_file,"Not Present in DB getting reviews data from web application")
                dataframe = self.get_reviews_from_ui()
                result = self.saveDataFrameDatatoFile("DataSet_Files_Collection/scrapper_data_"+searchstring+".csv",dataframe)
                mongoClient.insertRecordFromCSVFile(db_name="CarDekhoWebScrapping",collection_name=searchstring,csv_file="DataSet_Files_Collection/scrapper_data_"+searchstring+".csv",header=list(dataframe.columns))
            return searchstring
        except Exception as e:
            logger.app_logger().log(error_file,"Something went wrong on yeilding data %s:" % e)
            raise Exception(f"(getReviewsToDisplay) - Something went wrong on yielding data.\n" + str(e))

