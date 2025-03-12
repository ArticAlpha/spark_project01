from src.main.utility.spark_session import spark_session
from loguru import logger



def read_parquet_file(file_path):
    """
    :param file_path: takes a file_path
    :return: dataframe
    """
    try:
        spark = spark_session()
        # read the csv file into a DataFrame
        logger.info(f"------Reading parquet file from path {file_path}------")
        df = spark.read.format("parquet") \
            .option("header", "true") \
            .option("multiline", "true") \
            .option("escape", "\"") \
            .load(file_path)

        logger.info("------parquet file read successfully------")
        return df

    except Exception as e:
        logger.error(f"Error reading the parquet file:{e}")
        return None


if __name__ == "__main__":
    file_path = "E:\\spark_project01\\files\\transformed_data\\parquet\\"
    obj1 = read_parquet_file(file_path)

