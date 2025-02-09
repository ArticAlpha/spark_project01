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
    # print("spark session %s",spark)
    logger.info("------Spark session created successfully------")
    return spark
