# Mysql credentials
mysql_username="root"
mysql_password="Password"
mysql_database = "sunny"

# data cleaning file location
csv_file_path = "E:/spark_project01/files/raw_files/sample_data.csv" #"E:/spark_project01/src/testing/car_sales.csv"
invalid_price_path = "E:/spark_project01/files/invalid_data/invalid_data.csv"
cleaned_data_path = "E:/spark_project01/files/cleaned_data/parquet" #will be used in data_transformation.py

# Data data_transformation file location
transformed_data_path ="E:/spark_project01/files/transformed_data/parquet/"

# Dimension table path
dimension_base_bath = "E:/spark_project01/files/processed/dimensions/dim_"

# Fact table path
fact_table_path = "E:/spark_project01/files/processed/transactional_fact"

# upload.py
transformed_data_folder = "E:/spark_project01/files/transformed_data/parquet"  # Replace with your file path
bucket_name = "sparks3bucketproj1"  # Replace with your bucket name
s3_folder = "transformed_data"  # Replace with the desired folder in the bucket
