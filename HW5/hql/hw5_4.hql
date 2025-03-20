/**
Jakob Balkovec
CPSC 4330
Mon Mar 10th 2025

HW5_4:

Dualcore recently started a loyalty program to reward their best customers. Dualcore has a
sample of the data (loyalty_data.txt) that contains information about customers who have signed
up for the program, including their customer ID, first name, last name, email, loyalty level,
phone numbers , a list of past order IDs, and a struct that summarizes the minimum, maximum,
average, and total value of past orders. You will create the table, populate it with the provided
data, and then run a few queries to reference some fields.

Write a query ‘hw5_4.hql’ to select the third element from the order_ids for customer ID
1200866.
*/

USE dualcore;

SELECT order_ids[2] /* 0-indexed */
FROM loyalty_program 
WHERE customer_id = 1200866;