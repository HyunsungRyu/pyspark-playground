import os
os.environ["PYSPARK_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession
import pyspark.sql.functions as f



if __name__ == "__main__":

# case 1
    # ss: SparkSession = SparkSession.builder \
    #     .master("local[*]") \
    #     .appName("spark RDD") \
    #     .getOrCreate()

    # sc: SparkContext = ss.sparkContext

    # text_file: RDD[str] = sc.textFile("MultidomainSituationalAssessmentReport1.txt")

    # counts = text_file \
    #     .flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a + b)

    # output = counts.collect()
    # for word, count in output:
    #     print(f"{word}: {count}")

    # ss.stop()


# case 2
    ss: SparkSession = SparkSession.builder \
    .master("local[*]") \
    .appName("spark sql") \
    .getOrCreate()
    
    df = ss.read.text("MultidomainSituationalAssessmentReport2.txt")
    
    df = df.withColumn('word', f.explode(f.split(f.col('value'), " "))) \
    .withColumn("count", f.lit(1)).groupby("word").sum()
    
    df.show()
    