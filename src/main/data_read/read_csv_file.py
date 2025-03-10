from enum import nonmember

from src.main.utility.spark_session import spark_session
from loguru import logger
from pyspark.sql.functions import *

def read_csv(spark,file_path):

    try:
        #read the csv file into a DataFrame
        logger.info(f"------Reading CSV file from path {file_path}------")
        df=spark.read.format("csv")\
                .option("header","true")\
                .option("infer_schema","true")\
                .option("multiline","true")\
                .option("escape","\"")\
                .load(file_path)

        logger.info("------CSV file read successfully------")
        unique_reps=df.select("discounted_price").distinct()
        # print(unique_reps.count())
        # input()
        return unique_reps

    except Exception as e:
        logger.error(f"Error reading the csv file:{e}")
        return None

spark=spark_session()
csv_file_path= "E:/spark_project01/src/testing/car_sales.csv"
a = read_csv(spark,csv_file_path)
a.show()