from src.main.utility.my_sql_connectivity.database_connector import get_mysql_connection
from loguru import logger

def create_table_query():

    connection = get_mysql_connection()
    try:

        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE fact_orders (
    order_id INT,
    car_id INT,
    color_id INT,
    customer_id INT,
    order_status_id INT,
    sales_rep_id INT,
    payment_method_id INT,
    showroom_id INT,
    gender_id INT,
    marital_status_id INT,
    discounted_price DECIMAL(10, 2),
    order_date DATE,
    expected_delivery_date DATE,
    commission_obtained DECIMAL(10, 2),
    warranty_period VARCHAR(255),
    warranty_expiration_date DATE,
    profit_margin DECIMAL(10, 2),
    vin VARCHAR(255),

    FOREIGN KEY (car_id) REFERENCES dim_car(car_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (order_status_id) REFERENCES dim_order_status(order_status_id),
    FOREIGN KEY (sales_rep_id) REFERENCES dim_sales_rep(sales_rep_id),
    FOREIGN KEY (payment_method_id) REFERENCES dim_payment_method(payment_method_id),
    FOREIGN KEY (showroom_id) REFERENCES dim_showroom(showroom_id),
);
 """
        cursor.execute(create_table_query)
    except Exception as e:
        logger.error(f"Unable to create fact_orders table: {str(e)}")



def drop_table():

    connection = get_mysql_connection()
    try:

        cursor = connection.cursor()
        enable_query = f"DROP TABLE IF EXISTS fact_orders"
        cursor.execute(enable_query)
    except Exception as e:
        logger.error(f"Unable to drop fact_orders: {str(e)}")


if __name__=="__main__":
    create_table_query()
    drop_table()