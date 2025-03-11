from src.main.utility.spark_session import spark_session
from loguru import logger
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import *
from src.main.data_read.read_parquet import read_parquet_file


class Dimensions:
    def __init__(self, file_path):
        self.spark = spark_session()
        self.df = read_parquet_file(file_path)



    def create_dim_table(self, list_of_columns, table_name, schema):

        # columns = list_of_columns.split(",")
        if self.df is None:
            logger.error("No file present to process")
        try:
            logger.info(f"------ creating {table_name} dimension -------")
            distinct_df = self.df.select(list_of_columns).distinct()

            # creating empty dataframe
            empty_data_df = self.spark.createDataFrame([], schema)

            aligned_df = empty_data_df.unionByName(distinct_df, allowMissingColumns=True)

            aligned_df = aligned_df.withColumn(f"{table_name}_id",
                                               (monotonically_increasing_id() + 1).cast(IntegerType()))

            aligned_df = aligned_df.select(*[field.name for field in schema.fields])

            aligned_df.write.mode("overwrite").parquet(
                f"E:\\spark_project01\\files\\processed\\dimensions\\dim_{table_name}")
            aligned_df.show()
            logger.info(f"------ {table_name} dimension created successfully ------")

        except Exception as e:
            logger.error(f"------Error occurred while creating dimension: {table_name} error as {str(e)}")

    def create_all_dims(self):

        dim_configurations = [
            (
                ["make", "model", "color", "engine_type", "mileage", "fuel_type", "price"],
                "car",
                StructType([
                    StructField("car_id", IntegerType(), False),
                    StructField("make", StringType(), True),
                    StructField("model", StringType(), True),
                    StructField("color", StringType(), True),
                    StructField("engine_type", StringType(), True),
                    StructField("mileage", IntegerType(), True),
                    StructField("fuel_type", StringType(), True),
                    StructField("price", StringType(), True)
                ])
            ),
            (
                ["showroom_name", "showroom_address", "showroom_pincode", "showroom_phone"],
                "showroom",
                StructType([
                    StructField("showroom_id", IntegerType(), False),
                    StructField("showroom_name", StringType(), True),  # Name of the showroom
                    StructField("showroom_address", StringType(), True),  # Address of the showroom
                    StructField("showroom_pincode", IntegerType(), True),  # Pincode of the showroom's location
                    StructField("showroom_phone", StringType(), True)  # Phone number of the showroom
                ])
            ),
            (
                ["sales_rep_name", "sales_rep_phone", "sales_rep_email"],
                "sales_rep",
                StructType([
                    StructField("sales_rep_id", IntegerType(), False),  # Unique ID for the sales rep
                    StructField("sales_rep_name", StringType(), True),  # Name of the sales representative
                    StructField("sales_rep_phone", StringType(), True),  # Phone number of the sales representative
                    StructField("sales_rep_email", StringType(), True)  # Email address of the sales representative
                ])
            ),
            (
                ["customer_name", "customer_age", "customer_email", "customer_phone", "customer_address",
                 "customer_marital_status"],
                "customer",
                StructType([
                    StructField("customer_id", IntegerType(), False),  # Unique ID for the customer
                    StructField("customer_name", StringType(), True),  # Name of the customer
                    StructField("customer_age", IntegerType(), True),  # Age of the customer
                    StructField("customer_email", StringType(), True),  # Email address of the customer
                    StructField("customer_phone", StringType(), True),  # Phone number of the customer
                    StructField("customer_address", StringType(), True),  # Address of the customer
                    StructField("customer_marital_status", StringType(), True)  # Marital status of the customer
                ])
            ),
            (
                ["payment_method"],
                "payment_method",
                StructType([
                    StructField("payment_method_id", IntegerType(), False),  # Unique ID for the payment method
                    StructField("payment_method", StringType(), True)  # Payment method (e.g., "Credit Card", "Cash")
                ])
            ),
            (
                ["order_status"],
                "order_status",
                StructType([
                    StructField("order_status_id", IntegerType(), False),  # Unique ID for the order status
                    StructField("order_status", StringType(), True)
                    # Status of the order (e.g., "Completed", "Pending")
                ])
            )
        ]

        # Loop through each dimension configuration and create dimension tables
        for columns, table_name, schema in dim_configurations:
            self.create_dim_table(list_of_columns=columns, table_name=table_name, schema=schema)

file_path = "E:\\spark_project01\\files\\transformed_data\\parquet\\"
instance1 = Dimensions(file_path)

dims = instance1.create_all_dims()

# dim_df = instance1.create_dim_table(["make","model","color","engine_type","mileage","fuel_type","price"],"car",StructType([
#     StructField("car_id", IntegerType(), False),
#     StructField("make", StringType(), True),
#     StructField("model", StringType(), True),
#     StructField("color", StringType(), True),
#     StructField("engine_type", StringType(), True),
#     StructField("mileage", IntegerType(), True),
#     StructField("fuel_type", StringType(), True),
#     StructField("price", StringType(), True)
# ]))
