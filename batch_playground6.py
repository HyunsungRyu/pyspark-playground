import os
os.environ["PYSPARK_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/git_files/pyspark-playground/venv/Scripts/python.exe"
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def load_user_visits(ss: SparkSession):
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("visits", IntegerType(), False)
    ])
    
    data = [
        (1, 10),
        (2, 20),
        (3, 2),
        (4, 5),
        (5, 62),
        (6, 4)
    ]
    return ss.createDataFrame(data, schema)


def load_user_names(ss: SparkSession):
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False)
    ])
    data = [
        (1, 'signor'),
        (2, 'de Luca'),
        (3, 'mr'),
        (4, 'Ryu'),
        (6, 'Hyun'),
        (7, 'Erre')
    ]
    return ss.createDataFrame(data, schema)


if __name__ == "__main__":
    ss:SparkSession = SparkSession.builder\
        .master("local")\
        .appName("dataframe join")\
        .getOrCreate()

    user_visits_df = load_user_visits(ss)
    user_names_df = load_user_names(ss)

    # # 컬럼 지정 x : cartesian join(row : n*m)
    # user_names_df.join(user_visits_df).show()
    
    user_names_df.join(user_visits_df, on="id").show()
    # user_names_df.join(user_visits_df, on="id", how="left").show()
    # user_names_df.join(user_visits_df, on="id", how="right").show()
    user_names_df.join(user_visits_df, on="id", how="full").show()
    