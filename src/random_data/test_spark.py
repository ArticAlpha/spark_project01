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
        .appName("sunny_spark01") \
        .config("spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-java-8.0.26.jar") \
        .config("spark.sql.debug.maxToStringFields", "200") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .getOrCreate()

    # Write Parquet file with decimals
    df = spark.createDataFrame([(1.23,), (4.56,)], ["decimal_col"])
    df.write.mode("overwrite").parquet("E:\\spark_project01\\src\\random_data\\files")
    logger.info("Parquet file written successfully!")

    # Validate Parquet file
    # df_read = spark.read.parquet("C:\\tmp\\parquet_output")
    # df_read.show()

    logger.info("------Spark session created successfully------")
    return spark


obj1 = spark_session()
