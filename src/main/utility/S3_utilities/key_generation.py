from cryptography.fernet import Fernet

def generate_key():
    key = Fernet.generate_key()
    with open("/resources/dev/encryption.key", "wb") as key_file:
        key_file.write(key)
    print("Encryption Key saved to encryption.key")



if __name__=="__main__":
    generate_key()