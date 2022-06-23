# https://stackoverflow.com/questions/58415928/spark-s3-error-java-lang-classnotfoundexception-class-org-apache-hadoop-f
import os
import configparser

from pyspark.sql import SparkSession

import process
import utils

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
validCount=config['KEYS']['VALID_COUNT']
    
    
    
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

    return process.writeParquetData(productData, False, targetField, path)


def createEventTable(dataFrame, path):
    """
    Function: select event table and write data
    @params
    dataFrame: spark dataFrame
    path: path where the data writes
    """
    eventData = process.processEventData(dataFrame)
    
    return process.writeParquetData(eventData, False, '', path)


def createUserTable(dataFrame, path):
    """
    Function: select user table and write data
    @params
    dataFrame: spark dataFrame
    path: path where the data writes
    """
    userData = process.processUserData(dataFrame)
    return process.writeParquetData(userData, False, '', path)


def runDataQualityCheck(dataFrame, info, count):
    """
    Function:
     @params
     dataFrame: spark dataFrame
     format: data format
     path: path to locate data
     info: table name with primary key (dictionary type)
     """

    return utils.validateData(dataFrame, info, count)


def main():
    
    spark = sparkSession()
 
    # load Raw data
    rawData = process.loadData(spark, csvFormat, s3Url)
    mainData = rawData.drop_duplicates()

    #productPath = s3Url+product
    productPath = utils.createPath(False, s3Url, product)
    createProductTable(mainData, productPath)
    
    #eventPath = s3Url+event
    eventPath = utils.createPath(False, s3Url, event)
    createEventTable(mainData, eventPath)
    
    #userPath = s3Url+user
    userPath = utils.createPath(False, s3Url, user)
    createUserTable(mainData, userPath)

    productInfo = {'product':'product_id'}
    productList = process.loadData(spark, parquetFormat, productPath)
    runDataQualityCheck(productList, productInfo, validCount)

    eventInfo = {'product':'product_id', 'user':'user_session'}
    eventList = process.loadData(spark, parquetFormat, eventPath)
    runDataQualityCheck(eventList, eventInfo, validCount)

    userInfo = {'user':'user_session'}
    userList = process.loadData(spark, parquetFormat, userPath)
    runDataQualityCheck(userList, userInfo, validCount)

    #Transformation
    targetData = process.filterTargetData(productList, targetField, targetBrand)
    createCategory = process.transformProductData(targetData)
    createTimeUnit = process.transformEventData(eventList)

    #Summary target data
    summaryData = process.summaryTargetData(createCategory, createTimeUnit)
 
    #Data Qualtiy Check_2 : To ensure validation tables are normal
    summaryInfo = {'product':'product_id', 'user':'user_session'}

    runDataQualityCheck(summaryData, summaryInfo, validCount)
     
    #Summary result
    summaryPath = utils.createPath(False , s3Url , summary)
    process.writeCsvData(summaryData, summaryPath)

    spark.stop()

if __name__ == "__main__":
    main()
    
    

    