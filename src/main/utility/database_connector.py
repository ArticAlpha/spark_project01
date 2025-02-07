import mysql.connector
import configparser

base_path = "E:\\spark_project01\\resources\\dev\\"

def get_mysql_connection():
    config_path = f"{base_path}config.ini"
    config = configparser.ConfigParser()
    config.read(config_path)

    connection = mysql.connector.connect(
        host="localhost",
        user=config['mysql']['username'],
        password=config['mysql']['password'],
        database="sunny"
    )
    return connection



if __name__=="__main":
    get_mysql_connection()