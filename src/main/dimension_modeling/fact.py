from xmlrpc.client import FastParser

from src.main.utility.spark_session import spark_session
from loguru import logger
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import *
from src.main.data_read.read_parquet import read_parquet_file


#
# class Facts:
#     def __init__(self, file_path):
#         self.spark = spark_session()
#         self.df = read_parquet_file(file_path)


# spark=spark_session()


import mysql.connector
import configparser


def read_table_info():
    base_path = "E:\\spark_project01\\resources\\dev\\"
    config_path = f"{base_path}config.ini"
    config = configparser.ConfigParser()
    config.read(config_path)
    try:
        # Establish the connection
        connection = mysql.connector.connect(
            host="localhost",
            user=config['mysql']['username'],
            password=config['mysql']['password'],
            database="sunny"
        )

        if connection.is_connected():
            print("Successfully connected to the database")

            # Create a cursor object
            cursor = connection.cursor(dictionary=True)

            # Execute the query to read the table
            cursor.execute("SELECT * FROM dim_paths")

            # Fetch all rows from the table
            records = cursor.fetchall()

            # Print the rows
            # for row in records:
            return(records)

            # Close the cursor
            # cursor.close()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed")

# Call the function to read the table
# read_table_info()
# read_dim(read_table_info())

def read_dim(records: list):
    dataframes = {}
    for record in records:
        table_name = record['table_name']
        path = record['path']
        dataframes[table_name] = read_parquet_file(path)
        # print(f"DataFrame for {table_name}:")
        # dataframes[table_name].limit(10).show()
    return dataframes


def fact(dim_dict):
    dim_color = dim_dict['dim_color']
    dim_car = dim_dict['dim_car']
    dim_customer = dim_dict['dim_customer']
    dim_order_status = dim_dict['dim_order_status']
    dim_payment_method = dim_dict['dim_payment_method']
    dim_sales_rep = dim_dict['dim_sales_rep']
    dim_showroom = dim_dict['dim_showroom']
    dim_gender = dim_dict['dim_gender']
    dim_marital_status = dim_dict['dim_marital_status']
    # dim_order_status.show(1000,truncate=False)
    processed_df = read_parquet_file("E:\\spark_project01\\files\\transformed_data\\parquet\\")
    #
    # # processed_df.limit(10).show()
    #
    fact_df = (
        processed_df
        .join(dim_car, processed_df["model"] == dim_car["model"], "left")
        .join(dim_color, processed_df["color"] == dim_color["color"], "left")
        .join(dim_customer, processed_df["customer_name"] == dim_customer["name"], "left")
        .join(dim_order_status, processed_df["order_status"] == dim_order_status["order_status"], "left")
        .join(dim_payment_method, processed_df["payment_method"] == dim_payment_method["payment_method"], "left")
        .join(dim_sales_rep, processed_df["sales_rep_name"] == dim_sales_rep["name"], "left")
        .join(dim_showroom, processed_df["showroom_name"] == dim_showroom["name"], "left")
        .join(dim_gender, processed_df["gender"] == dim_gender["gender"], "left")
        .join(dim_marital_status, processed_df["marital_status"] == dim_marital_status["marital_status"], "left")

        .select(
            processed_df["order_id"].alias("order_id"),
            dim_car["car_id"].alias("car_id"),
            # dim_car["model"].alias("model"),
            dim_color["color_id"].alias("color_id"),
            dim_customer["customer_id"].alias("customer_id"),
            dim_order_status["order_status_id"].alias("order_status_id"),
            dim_sales_rep["sales_rep_id"].alias("sales_rep_id"),
            dim_payment_method["payment_method_id"].alias("payment_method_id"),
            dim_showroom["showroom_id"].alias("showroom_id"),
            dim_gender["gender_id"].alias("gender_id"),
            dim_marital_status["marital_status_id"].alias("marital_status_id"),

            processed_df["discounted_price"].alias("discounted_price"),
            processed_df["order_date"].alias("order_date"),
            processed_df["expected_delivery_date"].alias("expected_delivery_date"),
            processed_df["commission_obtained"].alias("commission_obtained"),
            processed_df["warranty_period"].alias("warranty_period"),
            processed_df["warranty_expiration_date"].alias("warranty_expiration_date"),
            processed_df["commission_obtained"].alias("commission_obtained"),
            processed_df["profit_margin"].alias("profit_margin"),
            processed_df["vin"].alias("vin")
        )
    )
    #
    fact_df.distinct().show(50,truncate=False)




# print(read_table_info())
read_table_info()
dim_dicts = read_dim(read_table_info())
# print(fact(dim_dicts))
fact(dim_dicts)