from contextlib import nullcontext

from loguru import logger
import os
import time
import configparser
from src.main.utility.decrypt_s3_client_object import get_s3_client

logger.add("E:\\spark_project01\\src\\main\\logs\\file_download.log", rotation="10 MB", level="INFO")
class GetFiles:

    def __init__(self,bucket_name):
        """
        :param bucket_name: takes the name of the bucket
        calls the get_s3_client()
        """
        self.s3_client=get_s3_client()
        self.bucket_name=bucket_name

        if self.s3_client is None:
            logger.error("------ failed to return s3 client ------")
            raise
    def list_files(self):
        """
        :return: list of file names present in the bucket
        """

        try:
            logger.info("------ Getting the list of files ------")
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)

            # Extract file names(keys) if objects are present
            if 'Contents' in response:
                file_names = [obj['Key'] for obj in response['Contents']]
                return file_names
            logger.info("------ files fetched successfully and saved to path ------")


        except Exception as e:
            logger.error("------ Unable to list files in the directory ------")
            return str(e)
        # #variables initialization
        # print(bucket_name,file_names)



    def download_files(self,local_file_path):
        """
        :param local_file_path: path where we need to save s3 files
        :return: True if files download successfully and logging otherwise False and log the error
        """
        try:
            start_time=time.time()
            logger.info("------ Starting the files download ------")
            file_names=self.list_files()

            for file_name in file_names:
                self.s3_client.download_file(self.bucket_name,file_name,os.path.join(local_file_path,file_name))

            end_time = time.time()
            time_taken = end_time - start_time

            logger.info(f"------ Files downloaded successfully and saved to {local_file_path} and size is {time_taken} ------")
            return True
        except Exception as e:
            logger.error(f"An error occurred during file download: {str(e)}")
            return str(e)


if __name__ =="__main__":
    # GetFiles.list_files()
    local_file_path="E:\\spark_project01\\files\\raw_files"
    file_manager = GetFiles("sparks3bucketproj1")
    print(file_manager.download_files(local_file_path))