/**
Jakob Balkovec
CPSC 4330
Mon Mar 10th 2025

HW5_8:

In the database ‘dualcore’ (the one you created in exercise 13), create a table named ratings for
storing tab-delimited records using this structure:

[TABLE]

Populate the ‘ratings’ table directly by copying product ratings data of 2012 (“ratings_2012.txt”
posted on Canvas) to the table’s directory in HDFS. Similarly, populate the ‘ratings’ table by
copying product ratings data of 2013(“ratings_2013.txt” posted on Canvas) to the table’s
directory in HDFS.

Customer ratings and feedback are great sources of information for both customers and retailers
like Dualcore. However, customer comments are typically free-form text and must be handled
differently.

Before delving into analyzing customer comments, you will begin by analyzing the numeric
ratings customers have assigned to various products.
*/

USE dualcore;

CREATE TABLE ratings (
    posted TIMESTAMP,
    cust_id INT,
    prod_id INT,
    rating TINYINT,
    message STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA INPATH '/hw5/ratings_2013.txt' 
INTO TABLE ratings;

LOAD DATA INPATH '/hw5/ratings_2012.txt' 
INTO TABLE ratings;


