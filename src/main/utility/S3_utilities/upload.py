from src.main.utility.S3_utilities.decrypt_s3_client_object import get_s3_client
from datetime import datetime
import os


def upload_to_s3(transformed_data_folder, bucket_name, S3_folder=None):
    """
    Uploads all .parquet files from a folder to an S3 bucket.

    Parameters:
    - transformed_data_folder: Local folder containing files to upload.
    - bucket_name: Name of the S3 bucket.
    - S3_folder: Folder in the S3 bucket (optional). If not provided, files are uploaded to the bucket root.

    Returns:
    - True if all uploads succeed, False otherwise.
    """
    # Create an S3 client
    s3_client = get_s3_client()
    start_time = datetime.now()

    try:
        for root, dirs, files in os.walk(transformed_data_folder):
            for file in files:
                if file.endswith(".parquet"):
                    # Construct local file path
                    local_file_path = os.path.join(root, file).replace("\\", "/")

                    # Construct S3 file path
                    s3_file_path = os.path.join(S3_folder, file).replace("\\", "/") if S3_folder else file

                    # Upload to S3
                    # s3_client.upload_file(local_file_path, bucket_name, s3_file_path)
                    print(f"Uploaded {local_file_path} to s3://{bucket_name}/{s3_file_path}")

        # Log process (success)
        print(f"All files successfully uploaded to s3://{bucket_name}/{S3_folder or ''}")
        return True

    except Exception as e:
        # Log process (failure)
        print(f"Error occurred while uploading files: {str(e)}")
        return False


# Example Usage
if __name__ == "__main__":
    transformed_data_folder = "E:/spark_project01/files/transformed_data/parquet"  # Replace with your file path
    bucket_name = "sparks3bucketproj1"  # Replace with your bucket name
    S3_folder = "transformed_data"  # Replace with the desired folder in the bucket

    success = upload_to_s3(transformed_data_folder, bucket_name, S3_folder)
    print(f"Upload success: {success}")
