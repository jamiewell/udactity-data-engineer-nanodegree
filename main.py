# https://stackoverflow.com/questions/58415928/spark-s3-error-java-lang-classnotfoundexception-class-org-apache-hadoop-f
import os
import configparser

from datetime import datetime

from pyspark.sql import SparkSession

import process

config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID'] = config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['KEYS']['AWS_SECRET_ACCESS_KEY']

s3Url = config['KEYS']['AWS_S3_ENDPOINT']
    
product = config['KEYS']['PRODUCT_TABLE']
event = config['KEYS']['EVENT_TABLE']
user = config['KEYS']['USER_TABLE']
summary = config['KEYS']['SUMMARY']

csvFormat = config['KEYS']['CSV_FORMAT']
parquetFormat = config['KEYS']['PARQUET_FORAMT']

targetField = config['KEYS']['TAGET_FILED']
targetBrand = config['KEYS']['TAGET_BRAND']
count=config['KEYS']['VALID_COUNT']
    
    
    
def sparkSession():
    spark = SparkSession\
            .builder\
            .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
            .getOrCreate()

    return spark


def createProductTable(dataFrame, path):
    """
    Function: select product table and write data
    @params
    dataFrame: spark dataFrame
    path: path where the data writes
    """
    productData = process.processProductData(dataFrame)

    return process.writeData(productData, False, targetField, path)


def createEventTable(dataFrame, path):
    """
    Function: select event table and write data
    @params
    dataFrame: spark dataFrame
    path: path where the data writes
    """
    eventData = process.processEventData(dataFrame)
    
    return process.writeData(eventData, False, '', path)


def createUserTable(dataFrame, path):
    """
    Function: select user table and write data
    @params
    dataFrame: spark dataFrame
    path: path where the data writes
    """
    userData = process.processUserData(dataFrame)
        
    return process.writeData(userData, False, '', path)
    
    
def main():
    
    spark = sparkSession()
 
    # load Raw data
    rawData = process.loadData(spark, csvFormat, s3Url)
    
    #productPath = s3Url+product
    productPath = process.createPath(False, s3Url , product) 
    createProductTable(rawData, productPath)
    
    #eventPath = s3Url+event
    eventPath = process.createPath(False, s3Url , event) 
    createEventTable(rawData, eventPath)
    
    #userPath = s3Url+user
    userPath = process.createPath(False, s3Url , user) 
    createUserTable(rawData, userPath)
     
    #Load Product Data
    readProductPath = process.createPath(False , s3Url , product)   
    productList = process.loadData(spark, parquetFormat, readProductPath)
    # Load Event Data
    readEventPath = process.createPath(False , s3Url , event)   
    eventList = process.loadData(spark, parquetFormat, readEventPath)
    
    #Transformation    
    targetData = process.filterTargetData(productList, targetField, targetBrand)
    createCategory = process.transformProductData(targetData)
    createTime = process.transformEventData(eventList)
    
    
    #Summary target data
    summaryData = process.summaryTargetData(createCategory, createTime)
 
    #Data Qualtiy Check
    process.validateData(summaryData, count)
     
    #Summary result
    summaryPath = process.createPath(False , s3Url , summary) 
    process.writeData(summaryData, False, '', summaryPath)
    
    spark.stop()

if __name__ == "__main__":
    main()
    
    

    