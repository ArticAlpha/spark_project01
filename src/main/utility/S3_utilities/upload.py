from src.main.utility.S3_utilities.decrypt_s3_client_object import get_s3_client
from loguru import logger
from datetime import datetime
from src.main.logs.log_process import log_process
import os

def upload_to_s3(file_name, bucket_name, folder=None):
    """
    Uploads a file to an S3 bucket under a specified folder.

    Parameters:
    - file_name: Path to the file to upload.
    - bucket_name: Name of the S3 bucket.
    - folder: Folder in the S3 bucket (optional). If not provided, uploads to the root.

    Returns:
    - True if the upload was successful, False otherwise.
    """
    # Create an S3 client
    s3_client = get_s3_client()
    start_time = datetime.now()
    try:

        # If a folder is specified, include it in the object name
        object_name = file_name.split("/")[-1]  # Extract file name
        if folder:
            object_name = f"{folder}/{object_name}"  # Add folder prefix

        # Upload the file to S3
        s3_client.upload_file(file_name, bucket_name, object_name)
        logger.success(f"File '{file_name}' uploaded to bucket '{bucket_name}' in folder '{folder}'.")
        end_time = datetime.now()
        log_process(
            process_name="Dimension table creation",
            start_time=start_time,
            end_time=end_time,
            status="Success",
            records_processed=os.path.getsize(file_name),
            remarks=f"{file_name} uploaded successfully to s3"
        )
    except Exception as e:
        end_time = datetime.now()
        log_process(
            process_name="Dimension table creation",
            start_time=start_time,
            end_time=end_time,
            status="Failed",
            remarks=f"Unable to upload file {file_name} to s3"
        )
        logger.error(f"------ Error occurred while uploading the file to S3: {str(e)} ------")
        return False



# Example Usage
if __name__ == "__main__":
    file_name = "/files/transformed_data/parquet/part-00000-ed1c252d-c7ea-4d8b-9c5b-a9f28844dc78-c000.snappy.parquet"  # Replace with your file path
    bucket_name = "sparks3bucketproj1"  # Replace with your bucket name
    folder = "transformed_data"  # Replace with the desired folder in the bucket
    upload_to_s3(file_name, bucket_name, folder)
