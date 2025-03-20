USE dualcore;

CREATE TABLE products (
    prod_id INT,
    brand STRING,
    name STRING,
    price INT,
    cost INT,
    shipping_wt INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE 
LOCATION '/user/hive/warehouse/dualcore.db/products';