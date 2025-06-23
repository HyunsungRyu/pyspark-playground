import os
os.environ["PYSPARK_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder\
        .master("local")\
        .appName("rdd examples ver")\
        .getOrCreate()
    
    sc: SparkContext = ss.sparkContext
    log_rdd: RDD[str] = sc.textFile("c:\\data\\pyspark\\log.txt")
    
    print(f"count: {log_rdd.count()}")