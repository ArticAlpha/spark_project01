CREATE TABLE dim_color (
    color_id INT AUTO_INCREMENT PRIMARY KEY,
    color VARCHAR(255) NOT NULL
);

CREATE TABLE dim_customer (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    age INT NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL
);

CREATE TABLE dim_gender (
    gender_id INT AUTO_INCREMENT PRIMARY KEY,
    gender VARCHAR(255) NOT NULL
);

CREATE TABLE dim_marital_status (
    marital_status_id INT AUTO_INCREMENT PRIMARY KEY,
    marital_status VARCHAR(255) NOT NULL
);

CREATE TABLE dim_order_status (
    order_status_id INT AUTO_INCREMENT PRIMARY KEY,
    order_status VARCHAR(255) NOT NULL
);

CREATE TABLE dim_payment_method (
    payment_method_id INT AUTO_INCREMENT PRIMARY KEY,
    payment_method VARCHAR(255) NOT NULL
);
CREATE TABLE dim_department (
    department_id INT AUTO_INCREMENT PRIMARY KEY,
    department_name VARCHAR(255) NOT NULL
);
CREATE TABLE dim_showroom (
    showroom_id INT AUTO_INCREMENT PRIMARY KEY,
    showroom_name VARCHAR(255) NOT NULL,
    showroom_address VARCHAR(255) NOT NULL,
    showroom_pincode INT NOT NULL,
    showroom_phone VARCHAR(255) NOT NULL
);
CREATE TABLE dim_sales_rep (
    sales_rep_id INT AUTO_INCREMENT PRIMARY KEY,         -- Unique identifier for the sales representative
    sales_rep_name VARCHAR(255) NOT NULL,                -- Name of the sales representative
    sales_rep_phone VARCHAR(255) NOT NULL,               -- Phone number of the sales representative
    sales_rep_email VARCHAR(255) NOT NULL,               -- Email of the sales representative
    department_id INT,                                   -- Foreign key to the dim_department table
    FOREIGN KEY (department_id) REFERENCES dim_department(department_id) -- Reference to the department table
);
CREATE TABLE dim_customer (
    customer_id INT AUTO_INCREMENT PRIMARY KEY, -- Unique identifier for the customer
    name VARCHAR(100) NOT NULL,                 -- Customer name
    age INT,                                    -- Customer age
    email VARCHAR(255),                         -- Customer email
    phone VARCHAR(20),                          -- Customer phone
    address VARCHAR(255),                       -- Customer address
    gender_id INT,                              -- Foreign key to dim_gender
    marital_status_id INT,                      -- Foreign key to dim_marital_status
    FOREIGN KEY (gender_id) REFERENCES dim_gender(gender_id), -- Reference to gender table
    FOREIGN KEY (marital_status_id) REFERENCES dim_marital_status(marital_status_id) -- Reference to marital status table
);