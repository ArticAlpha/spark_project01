from cryptography.fernet import Fernet
import configparser
import os
from loguru import logger

#configure loguru to log messages to a file
logger.add("/src/main/logs/encryption.log", rotation="10 MB", level="INFO")
base_path = "/resources/dev\\"

#Encrypt AWS credentials and save them to a file
def encrypt_credentials():
    #read the encryption key
    logger.info("starting encryption for aws credentials")
    key_path = f"{base_path}encryption.key"
    if not os.path.exists(key_path):
        logger.error(f"encryption key file not found at {key_path}")
        raise FileNotFoundError(f"encryption key file not found at {key_path}")

    try:
        with open(key_path,"rb") as key_file:
            key = key_file.read()
        fernet=Fernet(key)


        #get path of the config.ini file
        config_path = f"{base_path}config.ini"
        if not os.path.exists(config_path):
            logger.critical(f"config file not found at {config_path}")

        # Load credentials from the configuration file
        config = configparser.ConfigParser()
        config.read(config_path)
        aws_credentials = {
        "aws_access_key_id" : config['aws']['aws_access_key_id'],
        "aws_secret_access_key" : config['aws']['aws_secret_access_key']
        }

        encrypted_data = fernet.encrypt(str(aws_credentials).encode())
        credentials_path = f"{base_path}credentials.enc"
        with open(credentials_path,"wb") as enc_file:
            enc_file.write(encrypted_data)
        logger.success("""AWS credentials encrypted and saved to credentials.enc.
        """)

    except Exception as e:
        logger.error(f"Error encrypting the aws keys")
        # print((f"Error encrypting the aws keys {str(e)}")) #can be removed
        raise


#encrypting credentials(run only once)
if __name__=="__main__":
    encrypt_credentials()

