from loguru import logger
from src.main.data_cleaning.data_cleaning import DataCleaning
from src.main.data_transformation.transformation import DataTransform
from src.main.dimension_modeling.dimension import Dimensions
from src.main.dimension_modeling.custom_dimensions import CustomDimensions
from src.main.dimension_modeling.fact import Facts
from resources.dev.load_config import load_config
from datetime import datetime
from src.main.utility.my_sql_connectivity.log_process import log_process
from src.main.utility.S3_utilities.upload import upload_to_s3
config = load_config()


def main():
    start_time = datetime.now()
    try:
        logger.info("------ Starting Data Processing Pipeline ------")

        # Step 1: Data Cleaning
        logger.info("------ Starting Data Cleaning ------")
        csv_file_path = config.csv_file_path
        cleaning_instance = DataCleaning()
        df = cleaning_instance.data_read(csv_file_path)

        if df is None:
            logger.error("Error reading the CSV file. Exiting pipeline.")
            return

        logger.info("------ Removing null or negative prices ------")
        cleaned_df = cleaning_instance.check_price(df)

        logger.info("------ Final data cleaning ------")
        processed_df = cleaning_instance.data_cleaning(cleaned_df)

        # Step 2: Data Transformation
        file_path = config.cleaned_data_path
        instance1 = DataTransform(file_path)
        transformed_df = instance1.data_transform()

        # Step 3: Dimension Table Creation
        file_path = config.transformed_data_path
        instance1 = Dimensions(file_path)
        dims = instance1.create_all_dims()

        # Step 4: Custom Dimension Creation
        instance1 = CustomDimensions()
        list1 = instance1.read_dimension_info()
        dim_dicts = instance1.read_dim(list1)
        instance1.create_custom_dim(dim_dicts)

        # Step 5: Fact Table Creation
        instance1 = Facts()
        list1 = instance1.read_table_info()
        dim_dicts = instance1.read_dim(list1)
        instance1.create_fact(dim_dicts)

        # Step 6: Uploading transformed data to S3
        transformed_data_folder = config.transformed_data_folder # Replace with your file path
        bucket_name = config.bucket_name  # Replace with your bucket name
        S3_folder = config.bucket_name
        upload_to_s3(transformed_data_folder, bucket_name, S3_folder)

        #ending time of the whole process
        end_time = datetime.now()

        log_process(
           process_name="ETL Pipeline Execution",
           start_time=start_time,
           end_time=end_time,
           status="Success",
           remarks="ETL pipeline executed successfully."
        )


    except Exception as e:
        end_time = datetime.now()
        log_process(
            process_name="ETL Pipeline Execution",
            start_time=start_time,
            end_time=end_time,
            status="Failed",
            error_message=str(e),
            remarks="Error occurred during the ETL pipeline execution."
        )
        logger.error(f"------ Pipeline encountered an error: {str(e)} ------")


if __name__ == "__main__":
    main()
