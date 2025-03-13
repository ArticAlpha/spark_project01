import mysql.connector
from resources.dev.load_config import load_config
from src.main.utility.spark_session import spark_session
from loguru import logger

config = load_config()

class JdbcConnection:

    def __init__(self):
        self.spark = spark_session()
        self.mysql_url = "jdbc:mysql://localhost:3306/sunny"
        self.mysql_properties = {
            "user": "root",
            "password": "Password",
            "driver": "com.mysql.cj.jdbc.Driver"
        }

    def jdbc_write_table(self,df,table_name):

        try:

            df.write.jdbc(
                url=self.mysql_url,
                table=table_name,
                mode="append",  # Modes: 'append', 'overwrite', 'ignore', 'error' (default)
                properties=self.mysql_properties
            )

        except Exception as e:
            logger.error(f"Error writing the data in the database: {str(e)}")


    def jdbc_read_table(self,table_name):
        try:

            df = self.spark.read.jdbc(
                url=self.mysql_url,
                table=table_name,
                properties=self.mysql_properties
            )

            return df

        except Exception as e:
            logger.error(f"Dimension table not found: {str(e)}")




if __name__=="__main__":
    instance1 = JdbcConnection()
    # df="pass a dataframe here"
    # instance1.jdbc_write_table(df,"dim_car")
    df = instance1.jdbc_read_table("dim_marital_status")



