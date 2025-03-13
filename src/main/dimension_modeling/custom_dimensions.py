from loguru import logger
from src.main.data_read.read_parquet import read_parquet_file
from src.main.utility.my_sql_connectivity.database_connector import get_mysql_connection
from pyspark.sql.types import StructField, IntegerType,StringType,StructType
from datetime import datetime
from src.main.logs.log_process import log_process
from resources.dev.load_config import load_config
from src.main.utility.my_sql_connectivity.database_jdbc_connection import JdbcConnection
from src.main.utility.my_sql_connectivity.truncate_table import truncate_table
from src.main.utility.my_sql_connectivity.drop_recreate_fact import generate_constraints,drop_constraints



#getting config details
config = load_config()

class CustomDimensions:
    def __init__(self):
        self.connection = get_mysql_connection()
        # getting path from config.py
        self.df = read_parquet_file(config.transformed_data_path)

    def read_dimension_info(self):

        start_time = datetime.now()
        try:

            if self.connection.is_connected():
                logger.success("------ Successfully connected to the database ------")

                # Create a cursor object
                cursor = self.connection.cursor(dictionary=True)

                # Execute the query to read the table
                cursor.execute("SELECT * FROM dimension_dependencies")

                # Fetch all rows from the table
                records = cursor.fetchall()

                # Print the rows
                # for row in records:
                end_time = datetime.now()
                log_process(
                    process_name="Get dimension details for fact table creation",
                    start_time=start_time,
                    end_time=end_time,
                    status="Success",
                    records_processed=len(records),
                    remarks=f"Dimension table details fetched successfully"
                )
                return(records)




        except Exception as e:
            end_time = datetime.now()
            log_process(
                process_name="Get dimension details for fact table creation",
                start_time=start_time,
                end_time=end_time,
                status="Failed",
                records_processed=0,
                remarks=f"Unable to fetch Dimension table details"
            )
            logger.error(f"Error: {e}")
        finally:
            if self.connection.is_connected():
                self.connection.close()
                logger.info("------ MySQL connection is closed ------")


    def read_dim(self,records):
        dataframes = {}
        if records:
            start_time = datetime.now()
            try:
                logger.info("------ Reading dimension tables ------")
                for record in records:
                    table_name = record['dependent_table']
                    jdbc_instance1 = JdbcConnection()
                    dataframes[table_name] = jdbc_instance1.jdbc_read_table(table_name)
                logger.success("------ Dimension tables read successfully ------")




                end_time=datetime.now()
                log_process(
                    process_name="Create Dataframes from dimension tables",
                    start_time=start_time,
                    end_time=end_time,
                    status="Success",
                    remarks=f"Dataframes created successfully"
                )

                return dataframes
            except Exception as e:
                end_time = datetime.now()
                log_process(
                    process_name="Create Dataframes from dimension tables",
                    start_time=start_time,
                    end_time=end_time,
                    status="Failed",
                    remarks=f"Unable to create Dataframes"
                )
                logger.error(f"------ Error reading the dimension tables {str(e)}")
        else:
            logger.error("------ Dimension tables provided to process ------")
            log_process(
                process_name="Create Dataframes from dimension tables",
                start_time=datetime.now(),
                end_time=datetime.now(),
                status="Failed",
                remarks=f"No records were provided to make dataframes"
            )



    def create_custom_dim(self,dim_dict):
        try:
            tables = ["fact_orders","dim_customer", "dim_sales_rep","dim_car"]
            # dropping constraints of the tables
            drop_constraints(tables)

            custom_dim_configurations = [
                (
                    ["sales_rep_name", "sales_rep_phone", "sales_rep_email","sales_rep_department"],
                    "sales_rep",
                    StructType([
                        StructField("sales_rep_name", StringType(), True),
                        StructField("sales_rep_phone", StringType(), True),
                        StructField("sales_rep_email", StringType(), True),
                        StructField("department_id", IntegerType(), True) # Replace sales_rep_department with department_id
                    ])
                ),
                ( ["make", "model", "engine_type", "mileage", "fuel_type", "price", "color"],
                "car",
                StructType([
                    StructField("make", StringType(), True),
                    StructField("model", StringType(), True),
                    StructField("engine_type", StringType(), True),
                    StructField("mileage", IntegerType(), True),
                    StructField("fuel_type", StringType(), True),
                    StructField("price", StringType(), True),
                    StructField("color_id", IntegerType(), True)  # Replace color with color_id
                ])
            ),
                (
                    ["customer_name", "customer_age", "customer_email", "customer_phone", "customer_address", "gender",
                     "marital_status"],
                    "customer",
                    StructType([
                        StructField("customer_name", StringType(), True),
                        StructField("customer_age", IntegerType(), True),
                        StructField("customer_email", StringType(), True),
                        StructField("customer_phone", StringType(), True),
                        StructField("customer_address", StringType(), True),
                        StructField("gender_id", IntegerType(), True),  # Replace gender with gender_id
                        StructField("marital_status_id", IntegerType(), True)
                        # Replace marital_status with marital_status_id
                    ])
                )
            ]

            if dim_dict:
                custom_dim_dicts = {}
                for list_of_columns, table_name, schema in custom_dim_configurations:
                    if table_name == "car":
                        dim_color = dim_dict['dim_color']
                        processed_df = self.df.select(list_of_columns).distinct()

                        custom_df = (
                        processed_df
                        .join(dim_color, processed_df["color"] == dim_color["color"], "left")
                        .select(
                            *[processed_df[col] for col in list_of_columns if col!='color'],
                            dim_color["color_id"].alias("color_id")
                        ))

                        custom_dim_dicts[table_name]=custom_df
                    elif table_name =="sales_rep":
                        dim_sales_rep = dim_dict['dim_department']
                        processed_df = self.df.select(list_of_columns).distinct()

                        custom_df = (
                            processed_df
                            .join(dim_sales_rep, processed_df["sales_rep_department"] == dim_sales_rep["department_name"], "left")
                            .select(
                                *[processed_df[col] for col in list_of_columns if col != 'sales_rep_department'],
                                dim_sales_rep["department_id"]
                            ))
                        custom_dim_dicts[table_name]=custom_df
                    else :
                        dim_gender = dim_dict['dim_gender']
                        dim_marital_status = dim_dict['dim_marital_status']
                        processed_df = self.df.select(list_of_columns).distinct()

                        custom_df = (
                            processed_df
                            .join(dim_gender,processed_df["gender"] == dim_gender["gender"], "left")
                            .join(dim_marital_status, processed_df["marital_status"] == dim_marital_status["marital_status"], "left")
                            .select(
                                *[processed_df[col] for col in list_of_columns if col != 'gender' and col!='marital_status'],
                                dim_gender["gender_id"],
                                dim_marital_status["marital_status_id"]
                            ))
                        custom_dim_dicts[table_name] = custom_df


                for key in custom_dim_dicts:
                    dimension_table = "dim_" + key
                    truncate_table(dimension_table)
                    jdbc_instance = JdbcConnection()
                    jdbc_instance.jdbc_write_table(custom_dim_dicts[key], dimension_table)
                    custom_dim_dicts[key].show(5)

        except Exception as e:
            logger.error(f"Error occurred while creating custom dimensions: {str(e)}")
        finally:
            logger.info("re-creating constraints")
            generate_constraints()



# print(read_table_info())
if __name__ =="__main__":
    instance1 = CustomDimensions()
    list1 = instance1.read_dimension_info()
    dim_dicts = instance1.read_dim(list1)
    instance1.create_custom_dim(dim_dicts)




