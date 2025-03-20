/**
Jakob Balkovec
CPSC 4330
Mon Mar 10th 2025

HW5_5:

For this problem, you will need customers, orders, order_details, and products tables that you
have prepared in exercise 13.

Write a HiveQL query ‘hw5_5.hql’ to find how many products have been bought by the
customer 1071189?
*/

USE dualcore;

SELECT COUNT(*) 
FROM order_details 
WHERE order_id IN (
    SELECT order_id FROM orders WHERE cust_id = 1071189
);