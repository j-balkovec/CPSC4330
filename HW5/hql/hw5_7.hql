/**
Jakob Balkovec
CPSC 4330
Mon Mar 10th 2025

HW5_7:

For this problem, you will need customers, orders, order_details, and products tables that you
have prepared in exercise 13.

Write a HiveQL query ‘hw5_7.hql’ to list the customers (cust_id only) who have not placed
any order.
*/

USE dualcore;

SELECT c.cust_id
FROM customers c
LEFT JOIN orders o
    ON c.cust_id = o.cust_id
WHERE o.order_id IS NULL;