import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from loguru import logger

def spark_session():
    logger.info("------Starting the spark session------")
    spark = SparkSession.builder.master("local[*]") \
        .appName("sunny_spark01")\
        .config("spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-java-8.0.26.jar") \
        .getOrCreate()
    # spark.conf.set("spark.sql.debug.maxToStringFields", "200")
    # spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
    # spark = SparkSession.builder.config("spark.driver.host","localhost") \
    #     .appName("sunny_spark01") \
    #     .getOrCreate()


    # print("spark session %s",spark)
    logger.info("------Spark session created successfully------")
    return spark
