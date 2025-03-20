/**
Jakob Balkovec
CPSC 4330
Mon Mar 10th 2025

HW5_1:

Dualcore recently started a loyalty program to reward their best customers. Dualcore has a
sample of the data (loyalty_data.txt) that contains information about customers who have signed
up for the program, including their customer ID, first name, last name, email, loyalty level,
phone numbers , a list of past order IDs, and a struct that summarizes the minimum, maximum,
average, and total value of past orders. You will create the table, populate it with the provided
data, and then run a few queries to reference some fields.

Write a HiveQL query ‘hw5_1.hql’ to create the table loyalty_program.
*/

USE dualcore;

CREATE TABLE loyalty_program (
    customer_id INT,
    first_name STRING,
    email STRING,
    last_name STRING,
    loyalty_level STRING,
    phone_numbers MAP<STRING, STRING>,
    order_ids ARRAY<INT>,
    order_summary STRUCT<
        min:DOUBLE,
        max:DOUBLE,
        avg:DOUBLE,
        total:DOUBLE>
)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;