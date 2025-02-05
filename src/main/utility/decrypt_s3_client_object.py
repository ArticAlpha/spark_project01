import ast
import boto3

from cryptography.fernet import Fernet

base_path = "E:\\spark_project01\\resources\\dev\\"

def decrypt_credentials():
    """
    :return: decrypted AWS credentials as dictionary from the encrypted file.
    """

    try:
        #read the encryption key
        key_path = f"{base_path}encryption.key"
        with open(key_path,"rb") as key_file:
            key=key_file.read()
        fernet=Fernet(key)

        #decryting the credentials
        credential_path=f"{base_path}credentials.enc"
        with open(credential_path,"rb") as enc_file:
            encrypted_data=enc_file.read()

        decrypted_data = fernet.decrypt(encrypted_data).decode()
        return ast.literal_eval(decrypted_data)  #convert string to dict

    except Exception as e:
        print(f"Error decrypting the data:{e}")


def get_s3_client():
    """
    :return: s3 client using the credentials in the config file
    """
    try:
        credentials = decrypt_credentials()
        if not credentials:
            raise ValueError("Failed to get decrypted AWS credentials")


        # create a session and s3 client
        session = boto3.Session(
            aws_access_key_id = credentials["aws_access_key_id"],
            aws_secret_access_key = credentials["aws_secret_access_key"],
            region_name = "eu-north-1"
        )
        # print("successfully got the s3 client")
        return session.client('s3')

    except  Exception as e:
        print(f"Error creating s3 client: {str(e)}")
        return None


if __name__ == "__main__":
    obj1=get_s3_client()
    print(obj1)

