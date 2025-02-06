import ast
import boto3
from cryptography.fernet import Fernet
from loguru import logger

base_path = "E:\\spark_project01\\resources\\dev\\"
#configure loguru to log messages to a file
logger.add("E:\\spark_project01\\src\\main\\logs\\decryption.log", rotation="10 MB", level="INFO")
def decrypt_credentials():
    """
    :return: decrypted AWS credentials as dictionary from the encrypted file.
    """

    try:
        #read the encryption key
        logger.info("-------Starting decryption and initiating the s3 client-------")
        key_path = f"{base_path}encryption.key"
        with open(key_path,"rb") as key_file:
            key=key_file.read()
        fernet=Fernet(key)

        #decryting the credentials
        credential_path=f"{base_path}credentials.enc"
        with open(credential_path,"rb") as enc_file:
            encrypted_data=enc_file.read()

        decrypted_data = fernet.decrypt(encrypted_data).decode()
        logger.info("------Decryption of aws credentials is successful------")
        return ast.literal_eval(decrypted_data)  #convert string to dict

    except Exception as e:
        logger.error(f"Error decrypting the data:{e}")


def get_s3_client():
    """
    :return: s3 client using the credentials in the config file
    """
    try:
        credentials = decrypt_credentials()
        if not credentials:
            raise ValueError("Failed to get decrypted AWS credentials")

        logger.info("------Initiating s3 client------")
        # create a session and s3 client
        session = boto3.Session(
            aws_access_key_id = credentials["aws_access_key_id"],
            aws_secret_access_key = credentials["aws_secret_access_key"],
            region_name = "eu-north-1"
        )
        logger.info("------Successfully got the s3 client------")
        return session.client('s3')

    except  Exception as e:
        logger.error(f"Error creating s3 client: {str(e)}")
        return None


if __name__ == "__main__":
    obj1=get_s3_client()
    print(obj1)

