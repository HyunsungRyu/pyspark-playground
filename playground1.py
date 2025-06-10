import os
from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"

if __name__ == "__main__":

    ss: SparkSession = SparkSession.builder \
        .master("local[*]") \
        .appName("playground1") \
        .getOrCreate()

    sc: SparkContext = ss.sparkContext

    text_file: RDD[str] = sc.textFile("MultidomainSituationalAssessmentReport1.txt")

    counts = text_file \
        .flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)

    output = counts.collect()
    for word, count in output:
        print(f"{word}: {count}")

    ss.stop()
