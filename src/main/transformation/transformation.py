from src.main.utility.spark_session import spark_session
from loguru import logger
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import *
from src.main.data_read.read_parquet import read_parquet_file

class DataTransform:

    def __init__(self,file_path):
        self.spark=spark_session()
        self.df=read_parquet_file(file_path)

    def data_transform(self):
        """
        :param df:
        :return:
        """

        if self.df is None:
            logger.error("No dataframe provided to process")
            return None
        try:
            logger.info("------Processing dataframe------")
            # profit margin
            df_profit = self.df.withColumn("profit_margin",
                                      when(col("discounted_price")==0,0).otherwise(col("price")-col("discounted_price"))
                                      )

            #renaming delivery_date
            rename_df = df_profit.withColumnRenamed("delivery_date","expected_delivery_date")

            #calculating warranty period
            warranty_expiration_df = rename_df.withColumn("warranty_expiration_date",add_months(col("expected_delivery_date"),col("warranty_period")*12))
            gender_df = warranty_expiration_df.withColumnRenamed("customer_gender","gender")
            marital_status_df = gender_df.withColumnRenamed("customer_marital_status", "marital_status")

            marital_status_df.write.mode("overwrite").parquet("E:\\spark_project01\\files\\transformed_data\\parquet\\")

            logger.info(f"------ data transformed successfully ------")

        except Exception as e:
            logger.error(f"Error encountered: {str(e)}")



file_path = "E:\\spark_project01\\files\\cleaned_data\\parquet"
instance1 = DataTransform(file_path)
# df=instance1.data_read(file_path)
#Step 3 Final cleaning
transformed_df = instance1.data_transform()
# transformed_df.limit(20).show()
# transformed_df.select("mileage").distinct().show()

# dim_df = instance1.dim_tables(transformed_df,["make","model","color","engine_type","mileage","fuel_type","price"],"car",StructType([
#     StructField("car_id", IntegerType(), False),
#     StructField("make", StringType(), True),
#     StructField("model", StringType(), True),
#     StructField("color", StringType(), True),
#     StructField("engine_type", StringType(), True),
#     StructField("mileage", IntegerType(), True),
#     StructField("fuel_type", StringType(), True),
#     StructField("price", StringType(), True)
# ]))



# dim_df.limit(200).show()
