from src.main.utility.spark_session import spark_session
from loguru import logger
from pyspark.sql.functions import *
from src.main.data_read.read_parquet import read_parquet_file
from datetime import datetime
from src.main.logs.log_process import log_process
from resources.dev.load_config import load_config

#getting config details
config = load_config()

class DataTransform:

    def __init__(self,file_path):
        self.df=read_parquet_file(file_path)

    def data_transform(self):
        """
        :param df: takes a cleaned dataframe and transforms it
        :return: nothing, only write the transformed data in the form of parquet
        """

        if self.df is None:
            logger.error("------ No dataframe provided to process ------")
            return None
        start_time = datetime.now()
        try:
            logger.info("------ Processing dataframe ------")
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

            transformed_data_path = config.transformed_data_path
            marital_status_df.write.mode("overwrite").parquet(transformed_data_path)

            end_time = datetime.now()
            logger.success(f"------ data transformed successfully ------")


            log_process(
                process_name="Data Transformation",
                start_time=start_time,
                end_time=end_time,
                status="Success",
                file_name=transformed_data_path,
                records_processed=marital_status_df.count(),
                remarks=f"Data transformed successfully and written to {transformed_data_path}"
            )

        except Exception as e:
            end_time = datetime.now()
            log_process(
                process_name="Data Transformation",
                start_time=start_time,
                end_time=end_time,
                status="Failed",
                error_message=str(e),
                remarks="Error during data data_transformation"
            )
            logger.error(f"------ Error encountered: {str(e)} ------")



if __name__ == "__main__":
    file_path = config.cleaned_data_path
    instance1 = DataTransform(file_path)
    # df=instance1.data_read(file_path)

    transformed_df = instance1.data_transform()
