USE dualcore;

CREATE TABLE order_details (
    order_id INT,
    prod_id INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE 
LOCATION '/user/hive/warehouse/dualcore.db/order_details';