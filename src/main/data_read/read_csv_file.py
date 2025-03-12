from enum import nonmember

from src.main.utility.spark_session import spark_session
from loguru import logger
from pyspark.sql.functions import *


def read_csv(file_path,schema):

    try:
        #read the csv file into a DataFrame
        spark = spark_session()
        df=spark.read.format("csv")\
                    .option("header","true")\
                    .schema(schema)\
                    .option("multiline","true")\
                    .option("escape","\"")\
                    .load(file_path)
        return df

    except Exception as e:
        logger.error(f"Error reading the csv file:{e}")
        return None


# csv_file_path= "E:/spark_project01/src/testing/car_sales.csv"
# a = read_csv(spark,csv_file_path)
# aa=a.select("make","model", "engine_type","mileage","fuel_type","price").distinct().count()
# print(aa)


if __name__ == "__main__":
    csv_file_path = "E:/spark_project01/src/testing/car_sales.csv"
    # Assuming schema is defined somewhere
    schema = None  # Replace with your actual schema
    a = read_csv(csv_file_path, schema)
    aa = a.select("make", "model", "engine_type", "mileage", "fuel_type", "price").distinct().count()
