import mysql.connector
from resources.dev.load_config import load_config
from src.main.utility.spark_session import spark_session

config = load_config()
def jdbc_mysql_connection(df,table_name):
    spark=spark_session()
    mysql_url = "jdbc:mysql://localhost:3306/sunny"
    mysql_properties = {
        "user": "root",
        "password": "Password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    # table = "process_logs"

    df.write.jdbc(
        url=mysql_url,
        table=table_name,
        mode="append",  # Modes: 'append', 'overwrite', 'ignore', 'error' (default)
        properties=mysql_properties
    )
    # df.show()


if __name__=="__main__":
    get_mysql_connection()


