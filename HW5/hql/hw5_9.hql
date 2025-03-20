/**
Jakob Balkovec
CPSC 4330
Mon Mar 10th 2025

HW5_9:

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

2. We observed earlier that customers are very dissatisfied with one of the products that
Dualcore sells. Although numeric ratings can help identify which product that is, they don’t
tell Dualcore why customers don’t like the product. We could simply read through all the
comments associated with that output to learn this information, but that approach doesn’t
scale. Now we want to analyze the comments to get more information on why customers
don’t like the product.

Write a HiveQL query ‘hw5_9.hql’ to find the five most common trigrams (three-word
combinations) in the comments for the product that you have identified in the previous
question. Products’ comments are in the “message” field of table ratings.
*/

-- id 1274673

USE dualcore;

/* prod_id from q1*/
SELECT EXPLODE(NGRAMS(SENTENCES(LOWER(message)), 3, 5)) 
FROM ratings 
WHERE prod_id = 1274673;