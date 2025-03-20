USE dualcore;

CREATE TABLE orders (
    order_id INT,
    cust_id INT,
    order_date TIMESTAMP
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE 
LOCATION '/user/hive/warehouse/dualcore.db/orders';