from src.main.utility.database_connector import get_mysql_connection
from loguru import logger

def truncate_table(table_name):

    connection = get_mysql_connection()
    try:

        cursor = connection.cursor()
        truncate_query = f"TRUNCATE TABLE {table_name};"
        cursor.execute(truncate_query)


    except Exception as e:
        logger.error(f"Unable to truncate table: {table_name} error: {str(e)}")


if __name__=="__main__":
    truncate_table("dim_car")