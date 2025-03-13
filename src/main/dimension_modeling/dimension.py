from src.main.utility.spark_session import spark_session
from loguru import logger
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import *
from src.main.data_read.read_parquet import read_parquet_file
from datetime import datetime
from src.main.logs.log_process import log_process
from resources.dev.load_config import load_config
from src.main.utility.database_jdbc_connection import jdbc_mysql_connection
from src.main.utility.truncate_table import truncate_table


#getting config details
config = load_config()

class Dimensions:

    def __init__(self, file_path):
        self.spark = spark_session()
        self.df = read_parquet_file(file_path)

    def create_dim_table(self, list_of_columns, table_name, schema, rename_columns=None):
        if self.df is None:
            logger.error("No file present to process")
        start_time = datetime.now()
        try:
            logger.info(f"------ creating {table_name} dimension -------")

            distinct_df = self.df.select(list_of_columns).distinct()

            # Rename columns if rename_columns is provided
            if rename_columns:
                for old_name, new_name in rename_columns.items():
                    distinct_df = distinct_df.withColumnRenamed(old_name, new_name)

            # creating empty dataframe
            empty_data_df = self.spark.createDataFrame([], schema)

            aligned_df = empty_data_df.unionByName(distinct_df, allowMissingColumns=True)
            aligned_df = aligned_df.select(*[field.name for field in schema.fields])


            dimension_table = "dim_"+table_name
            truncate_table(dimension_table)
            jdbc_mysql_connection(aligned_df,dimension_table)

            #logging
            end_time = datetime.now()
            log_process(
                process_name="Dimension table creation",
                start_time=start_time,
                end_time=end_time,
                status="Success",
                records_processed=aligned_df.count(),
                remarks=f"{table_name} dimension created successfully"
            )
            logger.success(f"------ {table_name} dimension created successfully ------")

        except Exception as e:
            end_time = datetime.now()
            log_process(
                process_name="Dimension table creation",
                start_time=start_time,
                end_time=end_time,
                status="Failed",
                error_message=f"{str(e)}",
                records_processed=0,
                remarks=f"Error occurred while creating dimension: {table_name} error as {str(e)}"
            )
            logger.error(f"------ Error occurred while creating dimension: {table_name} error as {str(e)} ------")




    def create_all_dims(self):
        dim_configurations = [
            (
                ["make", "model", "engine_type", "mileage", "fuel_type", "price"],
                "car",
                StructType([
                    # StructField("car_id", IntegerType(), False),
                    StructField("make", StringType(), True),
                    StructField("model", StringType(), True),
                    # StructField("color", StringType(), True),
                    StructField("engine_type", StringType(), True),
                    StructField("mileage", IntegerType(), True),
                    StructField("fuel_type", StringType(), True),
                    StructField("price", StringType(), True)
                ]),
                None  # No renaming for this dimension
            ),
            (
                ["showroom_name", "showroom_address", "showroom_pincode", "showroom_phone"],
                "showroom",
                StructType([
                    # StructField("showroom_id", IntegerType(), False),
                    StructField("name", StringType(), True),  # Name of the showroom
                    StructField("address", StringType(), True),  # Address of the showroom
                    StructField("pincode", IntegerType(), True),  # Pincode of the showroom's location
                    StructField("phone", StringType(), True)  # Phone number of the showroom
                ]),
                {"showroom_name": "name",
                 "showroom_address": "address",
                 "showroom_pincode": "pincode",
                 "showroom_phone":"phone"
                 }  # Rename sales_rep_name to name  # No renaming for this dimension
            ),
            (
                ["sales_rep_name", "sales_rep_phone", "sales_rep_email"],
                "sales_rep",
                StructType([
                    # StructField("sales_rep_id", IntegerType(), False),  # Unique ID for the sales rep
                    StructField("name", StringType(), True),  # Name of the sales representative
                    StructField("phone", StringType(), True),  # Phone number of the sales representative
                    StructField("email", StringType(), True)  # Email address of the sales representative
                ]),
                {"sales_rep_name": "name",
                 "sales_rep_phone": "phone",
                 "sales_rep_email": "email"
                 }  # Rename sales_rep_name to name
            ),
            (
                ["customer_name", "customer_age", "customer_email", "customer_phone", "customer_address"],
                "customer",
                StructType([
                    # StructField("customer_id", IntegerType(), False),  # Unique ID for the customer
                    StructField("name", StringType(), True),  # Name of the customer
                    StructField("age", IntegerType(), True),  # Age of the customer
                    StructField("email", StringType(), True),  # Email address of the customer
                    StructField("phone", StringType(), True),  # Phone number of the customer
                    StructField("address", StringType(), True)  # Address of the customer
                ]),
                {"customer_name": "name",
                 "customer_age": "age",
                 "customer_email": "email",
                 "customer_phone": "phone",
                 "customer_address": "address"}  # Rename customer details
            ),
            (
                ["payment_method"],
                "payment_method",
                StructType([
                    # StructField("payment_method_id", IntegerType(), False),  # Unique ID for the payment method
                    StructField("payment_method", StringType(), True)  # Payment method (e.g., "Credit Card", "Cash")
                ]),
                None  # No renaming for this dimension
            ),
            (
                ["order_status"],
                "order_status",
                StructType([
                    # StructField("order_status_id", IntegerType(), False),  # Unique ID for the order status
                    StructField("order_status", StringType(), True)
                    # Status of the order (e.g., "Completed", "Pending")
                ]),
                None  # No renaming for this dimension
            ),
            (
                ["color"],
                "color",
                StructType([
                    StructField("color", StringType(), True)
                ]),
                None  # No renaming for this dimension
            ),
            (
                ["gender"],
                "gender",
                StructType([
                    StructField("gender", StringType(), True)  # Marital status of the customer
                ]),
                None  # No renaming for this dimension
            ),
            (
                ["marital_status"],
                "marital_status",
                StructType([
                    StructField("marital_status", StringType(), True)  # Marital status of the customer
                ]),
                None  # No renaming for this dimension
            )
        ]

        # Loop through each dimension configuration and create dimension tables
        for columns, table_name, schema, rename_columns in dim_configurations:
            # if table_name=="car":
            self.create_dim_table(list_of_columns=columns, table_name=table_name, schema=schema, rename_columns=rename_columns)


if __name__=="__main__":
    file_path = config.transformed_data_path
    instance1 = Dimensions(file_path)
    dims = instance1.create_all_dims()
