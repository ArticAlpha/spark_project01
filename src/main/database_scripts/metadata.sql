---dim_dependencies
CREATE TABLE dimension_dependencies (
    dimension_name VARCHAR(50) NOT NULL,
    dependent_table VARCHAR(50) NOT NULL,
    PRIMARY KEY (dimension_name, dependent_table)
);


---logs
CREATE TABLE process_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY, -- Unique identifier for each log entry
    process_name VARCHAR(255) NOT NULL, -- Name of the process
    start_time TIMESTAMP NOT NULL, -- Start time of the process
    end_time TIMESTAMP NULL, -- End time of the process
    status VARCHAR(50) NOT NULL, -- Status of the process
    error_message TEXT NULL, -- Error message if the process failed
    file_name VARCHAR(255) NULL, -- Name of the file being processed
    records_processed INT NULL, -- Number of records processed
    remarks TEXT NULL -- Additional notes about the process
);

--- dim_details
CREATE TABLE dim_details (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    description TEXT
);