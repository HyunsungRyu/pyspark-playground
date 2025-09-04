import os
os.environ["PYSPARK_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp, max, min, mean, round, hour, minute, collect_set, count

def load_data(from_file: bool, ss: SparkSession, schema):
    if from_file:
        return ss.read.schema(schema).csv('data\\log.csv')
    memory = [
        ["1.1.1.1", "2023-02-26 04:15:21", "PATCH", "/users", "400", 61],
        ["2.2.2.2", "2023-02-26 04:15:22", "GET", "/users", "401", 73],
        ["3.3.3.3", "2023-02-26 04:15:23", "POST", "/parsers", "200", 34]
    ]
    return ss.createDataFrame(memory, schema)

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder\
        .master('local')\
        .appName("log dataframe ex")\
        .getOrCreate()
        
    
    fields = StructType([
        StructField("ip", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status", StringType(), False),
        StructField("latency", IntegerType(), False)
    ])
    
    from_file = True

    df = load_data(from_file, ss, fields)
    
    # df.show()
    # df.printSchema()
    
    def milliseconds_to_seconds(n) -> float:
        return n / 1000.0
    
    df = df.withColumn("latency_sec", milliseconds_to_seconds(df.latency))

    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    # df.show()
    # df.printSchema()
    
    # filter = df.where((df.status == "400") & (df.endpoint == '/users')) 
    # filter.show()
    
    # group_cols = ["method", "endpoint"]
    
    # df.groupBy(group_cols)\
    #     .agg(max("latency").alias("max_latency"),
    #          min("latency").alias("min_latency"),
    #          round(mean("latency"), 3).alias("mean_latency"))\
    #     .show()
    
    # group_cols = ["hour", "min"]
    # df = df.withColumn(
    #     "hour", hour(df.timestamp)
    # ).withColumn(
    #     "min", minute(df.timestamp)
    # ).groupby(group_cols).agg(
    #     collect_set('ip').alias("ip_list"),
    #     count("ip").alias("count_ip")
    # ).sort(group_cols).show()
    group_cols = ["hour", "min"]
    df = df.withColumn(
        "hour", hour(df.timestamp)
    ).withColumn(
        "min", minute(df.timestamp)
    ).groupby(group_cols).agg(
        collect_set('ip').alias("ip_list"),
        count("ip").alias("count_ip")
    ).sort(group_cols).show()
    
    while True:
        pass
        

