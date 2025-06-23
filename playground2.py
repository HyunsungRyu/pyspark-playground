import os
os.environ["PYSPARK_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession
from typing import List
from datetime import datetime

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder\
        .master("local")\
        .appName("rdd examples ver")\
        .getOrCreate()
    
    sc: SparkContext = ss.sparkContext
    log_rdd: RDD[str] = sc.textFile("c:\\data\\pyspark\\log.txt")
    
    # print(f"count: {log_rdd.count()}")
    
    # print each row
    # log_rdd.foreach(lambda row: print(row))
    

    # a) map
    # a-1) str row → List[str]
    def parse_line(row: str):
        return row.strip().split(" | ")
    
    parsed_log_rdd: RDD[List[str]] = log_rdd.map(parse_line)
    
    # parsed_log_rdd.foreach(print)
    
    # b) filter
    # b-1) filter for 404
    def get_only_404(row: List[str]):
        status_code = row[3]
        return status_code == "404"
    
    rdd_404 = parsed_log_rdd.filter(get_only_404)
    # rdd_404.foreach(print)
    
    # b-2) filter for 2xx
    def get_only_2xx(row: List[str]):
        status_code = row[3]
        return status_code.startswith("2")
    
    rdd_normal = parsed_log_rdd.filter(get_only_2xx)
    # rdd_normal.foreach(print)
    
    # b-3) post requests / only playbooks api
    def get_post_request_N_playbooks(row: List[str]):
        log = row[2].replace("\"", "")
        return log.startswith("POST") and "/playbooks" in log
    
    rdd_post_playbooks = parsed_log_rdd\
        .filter(get_post_request_N_playbooks)
    # rdd_post_playbooks.foreach(print)
    
    # c) reduce 
    # c-1) Count of requests by API method (POST/GET/PUT/PATCH/DELETE)
    def extract_api_method(row: List[str]):
        log = row[2].replace("\"", "")
        api_method = log.split(" ")[0]
        return api_method, 1
    
    rdd_count_by_api_method = parsed_log_rdd.map(extract_api_method)\
        .reduceByKey(lambda a, b: a + b).sortByKey()
        
    # rdd_count_by_api_method.foreach(print)
    
    # c-2) request counts by minute
    def extract_hour_N_minute(row: List[str]) -> tuple[str, int]:
        timestamp = row[1].replace("[", "").replace("]", "")
        date_format = '%d/%b/%Y:%H:%M:%S'
        date_time_obj = datetime.strptime(timestamp, date_format)
        return f"{date_time_obj.hour}:{date_time_obj.minute}", 1
    
    rdd_count_by_hour_N_minute = parsed_log_rdd.map(extract_hour_N_minute)\
        .reduceByKey(lambda a, b: a + b)\
        .sortByKey()
        
    rdd_count_by_hour_N_minute.foreach(print)