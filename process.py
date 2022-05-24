import os
import configparser

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id, col,when, split, coalesce
from pyspark.sql.functions import year, month, dayofmonth 
from pyspark.sql.types import TimestampType, StructType, StructField, DoubleType, StringType



def filterTargetData(dataFrame, field, name):
    """
    Function: filter dataframe
    @params
    dataframe: spark dataFrame
    field: target field to filter dataframe
    name: value to filter data
    """
    
    return dataFrame.filter( col(field) == name)


def loadData(spark, formatType ,path):
    """
    Function : read data from the selected storage
    @params 
    spark: sparkSession
    path: the selected storage
    """ 
    
    return spark.read.format(formatType)\
                        .option("header", 'true')\
                        .load(path)


def fillNullProductData(dataFrame):
    """
    Function : fill null data from the selected dataframs
    @params 
    dataFrame: spark dataframe
    """ 
    return dataFrame.fillna({'brand':'none','category_code':'category1.category2.category3'})


def processProductData(dataFrame):
    """
    Function : process product data to return unique data in selected fields
    @params 
    dataframe: spark dataframe
    """
    
    # Required : casting price data type
    productTable = dataFrame\
                    .withColumn("price", col("price").cast(DoubleType()))\
                    .select('product_id','category_id','category_code','brand','price').drop_duplicates()
    
    return fillNullProductData(productTable)
    
def processEventData(dataFrame): 
    """
    Function : process event data to return unique data in selected fields
    @params 
    dataframe: spark dataframe
    """
    
    return dataFrame.select('event_time','event_type','product_id','user_session').drop_duplicates()


def processUserData(dataFrame):
    """
    Function : process user data to return unique data in selected fields
    @params 
    dataframe: spark dataframe
    """ 
    
    return dataFrame.select('user_session','user_id').drop_duplicates()
 
    
def writeData(dataFrame, isPartitioned, partition, path):
    """
    Function : write data to the selected storage
    @params 
    dataframe: spark dataframe
    isPartitioned: boolean type. detect wheter required to partiton or not
    outputPath: storage where data take places
    key: folder name    
    """ 
    
    if(isPartitioned):
        return dataFrame.write.partitionBy(partition).parquet(path, mode='overwrite')
    else:
        return dataFrame.write.parquet(path, mode='overwrite')
        
    
def transformProductData(dataFrame):
    """
    transformation : Split category fields by 'category_code'
    action1) create new fields : category_major , category_middle, category_micro respectively
    action2) fill micro category if category code exists 2 words code only
    """

    ## Insert default values into null category 
    #fillCategory = dataFrame.fillna({'brand':'none','category_code':'category1.category2.category3'})
    
    splitCategory = dataFrame.withColumn("category_major", split(col("category_code"),'\.').getItem(0))\
                                .withColumn("category_middle", split(col("category_code"),'\.').getItem(1))\
                                .withColumn("category_micro", split(col("category_code"),'\.').getItem(2))\
                                .withColumn('category_micro', coalesce(  "category_micro",  "category_middle" ))\
                                .select('brand','category_major', 'category_middle', 'category_micro', 'category_id', 'product_id', 'price')
    return splitCategory


def transformEventData(dataFrame):
    """
    transformation : make event_time to get time unit respectively
    action1) create new fields : year, month, day
    """
    
    eventTime = dataFrame.withColumn("yyyyMmDd", split(col("event_time"),' ').getItem(0))\
                        .withColumn("year", year("yyyyMmDd"))\
                        .withColumn("month", month("yyyyMmDd"))\
                        .withColumn("day", dayofmonth("yyyyMmDd"))\
                        .select('event_time', 'year', 'month','day','event_type','product_id','user_session')

    return eventTime


def filterTargetData(dataFrame, field, name):
    """
    function: filter dataframe
    @params
    dataframe: spark dataFrame
    field: target field to filter dataframe
    name: value to filter data
    """
    
    return dataFrame.filter( col(field) == name)
  
    
def summaryTargetData(mainData, subData):
    """
    function : 
    
    """ 
    
    #joinData = mainData.join(mainData , mainData.product_id == subData.product_id , how='inner')
    joinData = mainData.alias("M").join( subData.alias("S"), mainData.product_id == subData.product_id, how='inner')
    
    return joinData.groupBy('day','event_type','category_micro')\
                    .sum('M.price')\
                    .withColumnRenamed("sum(price)", "totalSum") \
    
    
def validateData(data, count):
    """
    To run quailty check fucntion
    @params
    data: the list of dataframe
    """
    
    if not data:
        return
    else:
        for df in data:
            quality_checks(data, count)
            
            
def quality_checks(dataframe, count):
    """
    To check dimension tables to make sure tables completes normally.
    @params
    :dataframe: spark dataframe
    :tableName: table name
    """
    
    total_count = dataframe.count()
    if total_count != count:
        print(f"Data quality : Total records {total_count} with zero records!")
    else:
        print(f"Data quality : table with {total_count} records.")
    return 0
 
    
def createPath(isPartition, path, key):
    """
    function: create data path
    @params
    :isPartition: boolean type. make new path to read parquet files
    :path: input path
    """
    
    mainPath = ''
    if(isPartition):
        keyPath = path+key
        mainPath = os.path.join(keyPath, '*/*/') 
    else:
        mainPath = os.path.join(path, key) 
        
    return mainPath
        