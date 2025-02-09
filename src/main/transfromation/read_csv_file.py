from enum import nonmember

from src.main.utility.spark_session import spark_session
from loguru import logger

def read_csv(spark,file_path):

    try:
        #read the csv file into a DataFrame
        logger.info(f"------Reading CSV file from path {file_path}------")
        df=spark.read.csv(file_path,header=True, inferSchema=True)

        logger.info("------CSV file read successfully------")
        df.show()
        input()

    except Exception as e:
        logger.error(f"Error reading the csv file:{e}")
        return None

spark=spark_session()
csv_file_path="E:\\spark_project01\\src\\testing\\car_sales.csv"

read_csv(spark,csv_file_path)