import os
os.environ["PYSPARK_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"
from pyspark import SparkContext
from pyspark.sql import SparkSession
    
def load_data(from_file: bool, sc: SparkContext):
    if (from_file):
        return load_data_from_file(sc)
    else:
        return load_data_from_in_memory(sc)
    

def load_data_from_file(sc: SparkContext):
    return sc.textFile("C:\\git_files\\pyspark-playground\\data\\user_visits.txt")\
            .map(lambda v: v.split(",")),\
           sc.textFile("c:\\git_files\\pyspark-playground\\data\\user_names.txt")\
            .map(lambda x: x.split(","))

    
def load_data_from_in_memory(sc: SparkContext):
    user_visits = [
        (1, 10),
        (3, 2),
        (4, 5),
        (6, 62),
        (8, 4)
    ]
    
    user_names = [
        (1, 'luca'),
        (2, 'mario'),
        (3, 'signor'),
        (4, 'de'),
        (7, 'Hyun'),
        (9, 'Sung')
    ]

    return sc.parallelize(user_visits), sc.parallelize(user_names)

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder\
        .master('local')\
        .appName('rdd playground3')\
        .getOrCreate()
    sc: SparkContext = ss.sparkContext

    user_visits_rdd, user_names_rdd = load_data(True, sc)
    
    
    joined_rdd = user_names_rdd.join(user_visits_rdd).sortByKey()
    # print(joined_rdd.take(2))
    
    # result = joined_rdd.filter(lambda x: x[1][0] == 'Ryu').collect()
    # print(result)
    
    # inner = user_names_rdd.join(user_visits_rdd).sortByKey()
    # print(inner.collect())
    
    # left_outer = user_names_rdd.leftOuterJoin(user_visits_rdd).sortByKey()
    # print(left_outer.collect())
    
    # right_outer = user_names_rdd.rightOuterJoin(user_visits_rdd).sortByKey()
    # print(right_outer.collect())
    
    full_outer = user_names_rdd.fullOuterJoin(user_visits_rdd).sortByKey()
    print(full_outer.collect())
    
    while True:
        pass