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

1. Write a HiveQL query ‘hw5_8.hql’ to find the product with the lowest average rating among
products with at least 50 ratings.
*/

USE dualcore;

SELECT prod_id, AVG(rating) AS average 
FROM ratings 
GROUP BY prod_id 
HAVING COUNT(*) > 50 
ORDER BY average ASC 
LIMIT 1;