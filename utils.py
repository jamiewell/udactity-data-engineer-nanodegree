import os

def validateData(data, info, count):
    """
    To run quality check function
    @params
    data: the list of dataframe
    """
    if not data:
        return
    else:
        quality_checks(data, info, count)

def quality_checks(dataframe, info, count):
    """
    To check dimension tables to make sure tables completes normally.
    @params
    :dataframe: spark dataframe
    :tableName: table name
    """
    for key in info.keys():
        ## To ensure there is non value
        total_count = dataframe.count()
        if total_count != count:
            print(f"Data quality passed : Table {key} table with {total_count} records.")
        else:
            print(f"Data quality error : Table {key} Total records {total_count} with zero records!")
        ## To ensure pk is unique in data
        pkCount = dataframe.groupby(info[key]).count().where("count > 1").count()
        if pkCount != count:
            print(f"Data quality error : Table {key}  has duplicates : {pkCount}")
        else:
            print(f"Data quality passed : Table {key} has no duplicates  ")





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
