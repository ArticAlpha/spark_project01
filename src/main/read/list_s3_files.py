from src.main.utility.S3_utilities.decrypt_s3_client_object import get_s3_client
from loguru import logger

#configure loguru to log messages to a file
logger.add("E:\\spark_project01\\src\\main\\logs\\list_files.log", rotation="10 MB", level="INFO")

def list_files_in_bucket(bucket_name):
    """
    :param bucket_name: Name of the s3 bucket
    :return: A list of file names(keys) in the bucket
    """
    try:
        # get the s3 client
        logger.info("------getting the s3 client------")
        s3_client = get_s3_client()
        if s3_client is None:
            logger.error("------failed to return s3 client------")
            return []

        #retrieve the list of files
        response = s3_client.list_objects_v2(Bucket=bucket_name)

        #Extract file names(keys) if objects are present
        if 'Contents' in response:
            file_names = [obj['Key'] for obj in response['Contents']]
            # logger.info(f"files in bucket: {bucket_name}")
            # for file in file_names:
            #     print(file)
            return file_names

        else:
            logger.error(f"no files found in the bucket: {bucket_name}")
            return []

    except Exception as e:
        logger.error(str(e))
        return None

if __name__=="__main__":
    a = list_files_in_bucket("sparks3bucketproj1")
    print(a)
