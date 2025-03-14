from loguru import logger
from src.main.utility.my_sql_connectivity.database_connector import get_mysql_connection
from datetime import datetime

def log_process(process_name, start_time, end_time, status,
        error_message=None, file_name=None, records_processed=None, remarks=None):
    connection = get_mysql_connection()


    if connection.is_connected():
            logger.success("------ Successfully connected to the database ------")
            # Create a cursor object
            cursor = connection.cursor()

            try:
                query = """
                            INSERT INTO process_logs (
                                process_name, start_time, end_time, status,
                                error_message, file_name, records_processed, remarks
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                values = (
                    process_name,
                    start_time,
                    end_time,
                    status,
                    error_message,
                    file_name,
                    records_processed,
                    remarks,
                )
                cursor.execute(query, values)
                connection.commit()
                print(f"Process {process_name} logged successfully.")

            except Exception as e:
                print(f"Error logging process {process_name}: {str(e)}")
if __name__=="__main__":
    log_process(
        process_name="Data Transformation",
        start_time=datetime.now(),
        end_time=datetime.now(),
        status="Success",
        file_name="/files/transformed_data/parquet",
        records_processed=0,
        remarks=f"Data transformed successfully and written to E:\\spark_project01\\files\\transformed_data\\parquet"
    )