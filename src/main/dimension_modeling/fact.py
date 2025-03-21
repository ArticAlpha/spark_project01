from loguru import logger
from src.main.data_read.read_parquet import read_parquet_file
from src.main.utility.my_sql_connectivity.database_connector import get_mysql_connection
from datetime import datetime
from src.main.utility.my_sql_connectivity.log_process import log_process
from resources.dev.load_config import load_config
from src.main.utility.my_sql_connectivity.database_jdbc_connection import JdbcConnection
from src.main.utility.my_sql_connectivity.truncate_table import truncate_table

#getting config details
config = load_config()

class Facts:
    def __init__(self):
        self.connection = get_mysql_connection()
        # getting path from config.py
        self.df = read_parquet_file(config.transformed_data_path)

    def read_table_info(self):

        start_time = datetime.now()
        try:

            if self.connection.is_connected():
                logger.success("------ Successfully connected to the database ------")

                # Create a cursor object
                cursor = self.connection.cursor(dictionary=True)

                # Execute the query to read the table
                cursor.execute("SELECT * FROM dim_details")

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
                    table_name = record['table_name']
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



    def create_fact(self,dim_dict):

        if dim_dict:
            start_time = datetime.now()
            try:
                logger.info("------ Fact table creation initiated ------")
                dim_car = dim_dict['dim_car']
                dim_customer = dim_dict['dim_customer']
                dim_order_status = dim_dict['dim_order_status']
                dim_payment_method = dim_dict['dim_payment_method']
                dim_sales_rep = dim_dict['dim_sales_rep']
                dim_showroom = dim_dict['dim_showroom']
                # dim_order_status.show(1000,truncate=False)
                processed_df = self.df
                #
                # # processed_df.limit(10).show()
                #
                fact_df = (
                    processed_df
                    .join(dim_car, processed_df["model"] == dim_car["model"], "left")
                    .join(dim_customer, processed_df["customer_name"] == dim_customer["customer_name"], "left")
                    .join(dim_order_status, processed_df["order_status"] == dim_order_status["order_status"], "left")
                    .join(dim_payment_method, processed_df["payment_method"] == dim_payment_method["payment_method"], "left")
                    .join(dim_sales_rep, processed_df["sales_rep_name"] == dim_sales_rep["sales_rep_name"], "left")
                    .join(dim_showroom, processed_df["showroom_name"] == dim_showroom["showroom_name"], "left")

                    .select(
                        processed_df["order_id"].alias("order_id"),
                        dim_car["car_id"].alias("car_id"),
                        # dim_car["model"].alias("model"),
                        dim_customer["customer_id"].alias("customer_id"),
                        dim_order_status["order_status_id"].alias("order_status_id"),
                        dim_sales_rep["sales_rep_id"].alias("sales_rep_id"),
                        dim_payment_method["payment_method_id"].alias("payment_method_id"),
                        dim_showroom["showroom_id"].alias("showroom_id"),

                        processed_df["discounted_price"].alias("discounted_price"),
                        processed_df["order_date"].alias("order_date"),
                        processed_df["expected_delivery_date"].alias("expected_delivery_date"),
                        processed_df["commission_obtained"].alias("commission_obtained"),
                        processed_df["warranty_period"].alias("warranty_period"),
                        processed_df["warranty_expiration_date"].alias("warranty_expiration_date"),
                        processed_df["profit_margin"].alias("profit_margin"),
                        processed_df["vin"].alias("vin")
                        )
                    )

                # fact_df.write.mode("overwrite").parquet(config.fact_table_path)
                fact_df.distinct().show(10,truncate=False)
                truncate_table("fact_orders")
                jdbc_instance = JdbcConnection()
                jdbc_instance.jdbc_write_table(fact_df, "fact_orders")

                logger.success("------ Fact table created successfully ------")
                end_time = datetime.now()
                log_process(
                    process_name="Fact table creation",
                    start_time=start_time,
                    end_time=end_time,
                    status="Success",
                    file_name=config.fact_table_path,
                    records_processed=fact_df.count(),
                    remarks=f"Fact table created successfully"
                )
            except Exception as e:
                end_time = datetime.now()
                log_process(
                    process_name="Fact table creation",
                    start_time=start_time,
                    end_time=end_time,
                    status="Failed",
                    records_processed=0,
                    remarks=f"Fact table creation failed"
                )
                logger.error(f"------ Error occurred while creating fact table {str(e)} ------")
        else:
            logger.error("------ Unable to create fact table ------")


# print(read_table_info())
if __name__ =="__main__":
    instance1 = Facts()
    list1 = instance1.read_table_info()
    dim_dicts = instance1.read_dim(list1)
    # # print(fact(dim_dicts))
    instance1.create_fact(dim_dicts)