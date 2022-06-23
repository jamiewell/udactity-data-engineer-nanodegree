from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id, col,when, split, coalesce
from pyspark.sql.functions import year, month, dayofmonth 
from pyspark.sql.types import TimestampType, StructType, StructField, DoubleType, StringType


def filterNullValues(dataFrame, field):
    """
    Function: filter null values from dataframe
    @params
    dataframe: spark dataFrame
    field: target field to filter dataframe
    """
    return dataFrame.filter(col(field).isNotNull())

def filterTargetData(dataFrame, field, name):
    """
    Function: filter dataframe
    @params
    dataframe: spark dataFrame
    field: target field to filter dataframe
    name: value to filter data
    """
    
    return dataFrame.filter( col(field) == name)


def loadData(spark, formatType, path):
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

    productCode = dataFrame.select('product_id').distinct()

    # Required : casting price data type
    productData = dataFrame\
                    .withColumn("price", col("price").cast(DoubleType()))\
                    .select('product_id','category_id','category_code','brand','price').drop_duplicates()

    productTable = productCode.alias('m').join(productData.alias('s'), on=['product_id'], how='inner' )\
                                .select('m.product_id','category_id','category_code','brand','price')

    return fillNullProductData(productTable)
    
def processEventData(dataFrame): 
    """
    Function : process event data to return unique data in selected fields
    @params 
    dataframe: spark dataframe
    """
    productCode = dataFrame.select('product_id').distinct()
    eventData = dataFrame.select('event_time','event_type','product_id','user_session').drop_duplicates()

    eventTable = productCode.alias('m').join(eventData.alias('s'), on=['product_id'], how='inner' ) \
        .select('event_time','event_type','product_id','user_session')

    return eventTable


def processUserData(dataFrame):
    """
    Function : process user data to return unique data in selected fields
    @params 
    dataframe: spark dataframe
    """
    sessionCode = dataFrame.select('user_session').distinct()
    userData = dataFrame.select('user_session','user_id').drop_duplicates()

    userTable = sessionCode.alias('m').join(userData.alias('s'), on=['user_session'], how='inner' ) \
                .select('user_session','user_id')

    return userTable

def writeCsvData(dataFrame, outputPath):
    """
    Function : write csv data to the selected storage
    @params
    dataframe: spark dataframe
    outputPath: storage where data take places
    """
    return dataFrame.coalesce(1).write.option("header",True)\
        .csv(outputPath, mode='overwrite')

def writeParquetData(dataFrame, isPartitioned, partition, outputPath):
    """
    Function : write data to the selected storage
    @params
    dataframe: spark dataframe
    isPartitioned: boolean type. enable to partition or not
    outputPath: storage where data take places
    key: folder name
    """
    if(isPartitioned):
        print('Partitioned')
        return dataFrame.write.partitionBy(partition).parquet(outputPath, mode='overwrite')
    else:
        print('Non-Partitioned')
        return dataFrame.write.parquet(outputPath, mode='overwrite')
        
    
def transformProductData(dataFrame):
    """
    transformation : Split category fields by 'category_code'
    action1) create new fields : category_major , category_middle, category_micro respectively
    action2) fill micro category if category code exists 2 words code only
    """
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
    function : To aggregate input data
    @Params
    mainData: main data including selling price
    subData : sub Data with time units to join main data
    """
    #joinData = mainData.join(mainData , mainData.product_id == subData.product_id , how='inner')
    joinData = mainData.alias("M").join( subData.alias("S"), mainData.product_id == subData.product_id, how='inner')
    return joinData.groupBy('year','month','day','event_type','brand','category_micro')\
                    .sum('M.price')\
                    .withColumnRenamed("sum(price)", "totalSum")
