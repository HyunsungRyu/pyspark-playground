from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp, max, min, mean, round, hour, minute, collect_set, count
def load_data(from_file: bool, ss: SparkSession, schema):
    if from_file:
        return ss.read.schema(schema).csv("data\\log.csv")
    memory = [
        ["1.1.1.1", "2023-02-26 04:15:21", "PATCH", "/users", "400", 61],
        ["2.2.2.2", "2023-02-26 04:15:22", "GET", "/users", "401", 73],
        ["3.3.3.3", "2023-02-26 04:15:23", "POST", "/parsers", "200", 34]
    ]
    return ss.createDataFrame(memory, schema)


if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder\
        .master("local")\
        .appName("log sql")\
        .getOrCreate()
        
    from_file = True
    
    fields = StructType([
        StructField("ip", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status", StringType(), False),
        StructField("latency", IntegerType(), False)
    ])
    
    table_name = "log_data"
    
    load_data(from_file, ss, fields)\
        .createOrReplaceTempView(table_name)
        
    # ss.sql(f"select * from {table_name}").show()
    
    # ss.sql(f"select * from {table_name}").printSchema()
    
    # ss.sql(f"""
    #     select *, latency / 1000 as latency_sec
    #     from {table_name}   
    #     """).show()
    
    # ss.sql(f"""
    #        select ip, timestamp(timestamp) as timestamp, method, endpoint, status, latency
    #        from {table_name}
    #        """).printSchema()
    
    
    # ss.sql(f"""
    #        select * from {table_name}
    #        where status = "400" and endpoint = "/users"
    #        """).show()
    

    # ss.sql(f"""
    #     select
    #        method, endpoint,
    #        max(latency) as max_latency,
    #        min(latency) as min_latency,
    #        round(avg(latency), 3) as avg_latency
    #     from {table_name}
    #     group by method, endpoint
    #     """).show()
    
    ss.sql(f"""
        select
            hour(timestamp) as hour,
            minute(timestamp) as min,
            collect_set(ip) as ip_list,
            count(ip) as count_ip
        from {table_name}
        group by hour, min
        order by hour, min
    """).show()