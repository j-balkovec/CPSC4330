/**
Jakob Balkovec
CPSC 4330
Mon Mar 10th 2025

HW5_10:

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

3. Among the patterns you see in the result of the previous question is the phrase “ten times
more.” This might be related to the complaints that the product is too expensive. Write a
query ‘hw5_10.hql’ to list the product’s (the product that you have identified in question 1 of
this problem) comments that contain the phrase
*/

-- id 1274673

USE dualcore;

/** prod_id from q1*/
SELECT message 
FROM ratings
WHERE prod_id = 1274673 
AND message LIKE "%ten times more%";