from loguru import logger
from pyspark.sql.types import StructField, IntegerType, DateType, FloatType
from pyspark.sql.functions import *
from src.main.data_read.read_csv_file import read_csv
from datetime import datetime
from src.main.utility.my_sql_connectivity.log_process import log_process
from resources.dev.load_config import load_config

config = load_config()

class DataCleaning:

    def data_read(self,file_path):
        start_time = datetime.now()
        try:

            #read the csv file into a DataFrame
            logger.info(f"------ Reading CSV file from path {file_path} ------")

            schema = StructType([
                StructField("car_id", IntegerType(), True),
                StructField("make", StringType(), True),
                StructField("model", StringType(), True),
                StructField("year", DateType(), True),
                StructField("color", StringType(), True),
                StructField("price", IntegerType(), True),
                StructField("discounted_price", FloatType(), True),
                StructField("vin", StringType(), True),
                StructField("engine_type", StringType(), True),
                StructField("mileage", IntegerType(), True),
                StructField("fuel_type", StringType(), True),
                StructField("order_id", IntegerType(), True),
                StructField("order_date", DateType(), True),
                StructField("delivery_date", DateType(), True),
                StructField("showroom_name", StringType(), True),
                StructField("showroom_address", StringType(), True),
                StructField("showroom_pincode", IntegerType(), True),
                StructField("showroom_phone", StringType(), True),
                StructField("sales_rep_name", StringType(), True),
                StructField("sales_rep_phone", StringType(), True),
                StructField("sales_rep_email", StringType(), True),
                StructField("commission_obtained", FloatType(), True),
                StructField("sales_rep_department", StringType(), True),
                StructField("sales_rep_experience_years", IntegerType(), True),
                StructField("customer_name", StringType(), True),
                StructField("customer_age", IntegerType(), True),
                StructField("customer_email", StringType(), True),
                StructField("customer_phone", StringType(), True),
                StructField("customer_address", StringType(), True),
                StructField("customer_gender", StringType(), True),
                StructField("customer_marital_status", StringType(), True),
                StructField("order_amount", IntegerType(), True),
                StructField("order_status", StringType(), True),
                StructField("payment_method", StringType(), True),
                StructField("warranty_period", StringType(), True)
            ])

            df=read_csv(file_path, schema)
            logger.info("------ CSV file read successfully ------")

            end_time = datetime.now()

            log_process(
                process_name="Data Reading",
                start_time=start_time,
                end_time=end_time,
                status="Success",
                file_name=file_path,
                records_processed=df.count(),
                remarks="File read successfully"
            )
            return df

        except Exception as e:
            end_time = datetime.now()
            logger.error(f"------ Error reading the csv file:{e} ------")
            log_process(
                process_name="Data Reading",
                start_time=start_time,
                end_time=end_time,
                status="Failed",
                error_message=str(e),
                file_name=file_path,
                remarks="Error occurred during file reading"
            )
            return None



    #checking for null mileage, commission and cost of the vehicle
    def check_price(self,df):
        # Generate a condition to check if all columns are null
        """
            Checks for null or negative values in the 'price' column, saves those rows to a CSV file,
            and removes them from the original DataFrame.

            :param df: The input DataFrame.
            :param output_path: The file path where the invalid rows should be saved.
            :return: The cleaned DataFrame with invalid rows removed.
        """
        start_time = datetime.now()
        try:
            # dropping sales_rep_experice
            df = df.drop("sales_rep_experience_years")
            df = df.drop("order_amount")
            invalid_df = df.filter((col("price")<0) | col("price").isNull())


            if invalid_df.count()>0:
                path=config.invalid_price_path  #getting tht path from the config.py
                invalid_df.write.mode("overwrite").csv(path,header=True)
                print(f"------ Invalid data saved to: {path} ------")


            #remove invalid rows from original dataframe
            cleaned_df = df.filter(~(col("price")<0) | col("price").isNull())
            end_time = datetime.now()
            log_process(
                process_name="Price Validation",
                start_time=start_time,
                end_time=end_time,
                status="Success",
                records_processed=invalid_df.count(),
                remarks="Invalid prices removed"
            )
            return cleaned_df


        except Exception as e:
            end_time = datetime.now()
            print(f"Error Occurred {str(e)}")

            log_process(
                process_name="Price Validation",
                start_time=start_time,
                end_time=end_time,
                status="Failed",
                error_message=str(e),
                remarks="Error occurred during price validation"
            )
            return None


    def data_cleaning(self,df):
        """

        :param df: takes a dataframe and cleans it
        :return: nothing, only write the cleaned data in the form of parquet
        """

        if df is None:
            logger.error("------ No dataframe provided to process ------")
            return None
        start_time = datetime.now()
        try:
            logger.info("------ Processing dataframe ------")
            # processed_df = df.select("sales_rep_name","sales_rep_phone").distinct()

            #filing N/A where color is not present
            df = df.fillna({"color":"N/A"})

            # removing years from warranty period
            df = df.withColumn("warranty_period", coalesce(col("warranty_period"),lit(0)))
            df = df.withColumn("warranty_period", regexp_replace(col("warranty_period")," years",""))
            df = df.withColumn("warranty_period", col("warranty_period").cast(IntegerType()))


            filtered_mileage_df = df.select("mileage","model").filter(col("mileage").isNotNull())
            avg_mileage_df = filtered_mileage_df.groupBy(col("model")).agg(
                floor(avg(col("mileage"))).alias("avg_mileage")
            )

            df = df.join(avg_mileage_df, on="model", how="left").withColumn(
                "mileage", when(col("mileage").isNull(),col("avg_mileage")).otherwise(col("mileage"))
            )

            df=df.drop("avg_mileage")

            # adding default value of 0 to commission where commission is null
            df=df.fillna({"commission_obtained":0})

            #correcting order status
            order_status_df = df.withColumn("order_status", when(
                (col("payment_method").isNotNull()) & (col("order_status").isNull()), "Completed").otherwise(
                col("order_status")))

            order_status_df = order_status_df.withColumn("order_status", when(
                (col("payment_method").isNull()) & (col("order_status").isNull()), "Cancelled").otherwise(
                col("order_status")))

            final_status_df = order_status_df.withColumn("payment_method",
                              when((col("payment_method").isNull()) & (col("order_status") == "Cancelled"),"Not Required")
                              .when((col("payment_method").isNull()) & (col("order_status") == "Completed"), "Unknown")
                              .when((col("payment_method").isNull()) & (col("order_status") == "Pending"), "Not Initiated")
                              .otherwise(col("payment_method"))
                                                         )


            #adding defaults in customer_age
            final_df = final_status_df.fillna({"customer_age": 25})

            #adding custom as color where
            final_df = final_df.withColumn("color",when(col("color")=="N/A","Custom").otherwise(col("color")))

            cleaned_data_path = config.cleaned_data_path
            final_df.write.mode("overwrite").parquet(cleaned_data_path)
            ## logger.info(f"------Data cleaned and saved to: {cleaned_data_path}------")

            logger.info(f"------ Dataframe Processed and cleaned data is written to {cleaned_data_path} ------")

            end_time = datetime.now()
            log_process(
                process_name="Data Cleaning",
                start_time=start_time,
                end_time=end_time,
                status="Success",
                file_name=cleaned_data_path,
                records_processed=df.count(),
                remarks="Data cleaned successfully"
            )
            # return  final_df
        except Exception as e:
            end_time = datetime.now()
            logger.error(f"------ Error encountered: {str(e)} ------")
            log_process(
                process_name="Data Cleaning",
                start_time=start_time,
                end_time=end_time,
                status="Failed",
                error_message=str(e),
                remarks="Error occurred during data cleaning"
            )



if __name__ == "__main__":
    csv_file_path = config.csv_file_path
    instance1 = DataCleaning()
    df = instance1.data_read(csv_file_path)
    # df.show(5, truncate=False)
    if df is None:
        logger.error("------ Error reading the csv file -------")
        exit()


    # Step 2 removing rows where price is null or negative
    if df is None:
        logger.error("Error occurred during null removal")
    cleaned_df = instance1.check_price(df)


    #Step 3 Final cleaning
    processed_df = instance1.data_cleaning(cleaned_df)
