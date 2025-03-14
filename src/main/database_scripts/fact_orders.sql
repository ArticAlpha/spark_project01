CREATE TABLE fact_orders (
    order_id INT,
    car_id INT,
    customer_id INT,
    order_status_id INT,
    sales_rep_id INT,
    payment_method_id INT,
    showroom_id INT,
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
    FOREIGN KEY (showroom_id) REFERENCES dim_showroom(showroom_id)
);