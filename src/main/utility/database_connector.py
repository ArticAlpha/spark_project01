import mysql.connector
from resources.dev.load_config import load_config

config = load_config()
def get_mysql_connection():


    connection = mysql.connector.connect(
        host="localhost",
        user=config.mysql_username,
        password=config.mysql_password,
        database=config.mysql_database
    )
    return connection



if __name__=="__main__":
    get_mysql_connection()


