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

def drop_constraints(tables):
    """
    Drop all foreign key constraints for the given list of tables.

    Parameters:
    - tables: List of table names.
    - database_name: Name of the database where the tables are located.
    """
    # Get MySQL connection (update this according to your setup)
    connection = get_mysql_connection()  # Ensure this function returns a valid connection
    try:
        cursor = connection.cursor()

        for table in tables:
            # Fetch all foreign key constraints for the table
            query = f"""
            SELECT CONSTRAINT_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = '{table}' AND CONSTRAINT_NAME IS NOT NULL and CONSTRAINT_NAME !='PRIMARY';
            """
            cursor.execute(query)
            constraints = cursor.fetchall()

            for (constraint_name,) in constraints:
                # Generate and execute ALTER TABLE statement to drop the constraint
                drop_query = f"ALTER TABLE {table} DROP FOREIGN KEY {constraint_name};"
                cursor.execute(drop_query)
                print(f"Dropped constraint {constraint_name} from table {table}")



        # connection.commit()
        print("All constraints dropped successfully.")
        return constraints
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    # finally:
    #     if connection.is_connected():
    #         cursor.close()
    #         connection.close()
    #         print("MySQL connection closed.")



def generate_constraints():
    sql_statements = """
    -- Adding foreign key constraints for dim_customer
ALTER TABLE dim_customer ADD CONSTRAINT dim_customer_ibfk_1 FOREIGN KEY (gender_id) REFERENCES dim_gender(gender_id);
ALTER TABLE dim_customer ADD CONSTRAINT dim_customer_ibfk_2 FOREIGN KEY (marital_status_id) REFERENCES dim_marital_status(marital_status_id);

-- Adding foreign key constraints for dim_sales_rep
ALTER TABLE dim_sales_rep ADD CONSTRAINT dim_sales_rep_ibfk_1 FOREIGN KEY (department_id) REFERENCES dim_department(department_id);

-- Adding foreign key constraints for dim_car
ALTER TABLE dim_car ADD CONSTRAINT dim_car_ibfk_1 FOREIGN KEY (color_id) REFERENCES dim_color(color_id);

-- Adding foreign key constraints for fact_orders
ALTER TABLE fact_orders ADD CONSTRAINT fact_orders_ibfk_1 FOREIGN KEY (car_id) REFERENCES dim_car(car_id);
ALTER TABLE fact_orders ADD CONSTRAINT fact_orders_ibfk_2 FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id);
ALTER TABLE fact_orders ADD CONSTRAINT fact_orders_ibfk_3 FOREIGN KEY (order_status_id) REFERENCES dim_order_status(order_status_id);
ALTER TABLE fact_orders ADD CONSTRAINT fact_orders_ibfk_4 FOREIGN KEY (sales_rep_id) REFERENCES dim_sales_rep(sales_rep_id);
ALTER TABLE fact_orders ADD CONSTRAINT fact_orders_ibfk_5 FOREIGN KEY (payment_method_id) REFERENCES dim_payment_method(payment_method_id);
ALTER TABLE fact_orders ADD CONSTRAINT fact_orders_ibfk_6 FOREIGN KEY (showroom_id) REFERENCES dim_showroom(showroom_id);

        """
    connection = get_mysql_connection()
    cursor = connection.cursor()
    cursor.execute(sql_statements)

    # Example Usage


if __name__ == "__main__":
    # List of tables to drop constraints from
    tables = ["dim_customer"]
    # Call the function
    # a= drop_constraints(tables)
    # print(a)
    generate_constraints()
# if __name__=="__main__":
#     create_table_query()
#     drop_table()