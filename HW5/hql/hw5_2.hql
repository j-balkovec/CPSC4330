/**
Jakob Balkovec
CPSC 4330
Mon Mar 10th 2025

HW5_2:

Dualcore recently started a loyalty program to reward their best customers. Dualcore has a
sample of the data (loyalty_data.txt) that contains information about customers who have signed
up for the program, including their customer ID, first name, last name, email, loyalty level,
phone numbers , a list of past order IDs, and a struct that summarizes the minimum, maximum,
average, and total value of past orders. You will create the table, populate it with the provided
data, and then run a few queries to reference some fields.

Write a query ‘hw5_2.hql’ to load the data in loyalty_data.txt (posted on Canvas) into Hive.
*/

USE dualcore;

LOAD DATA INPATH '/hw5/loyalty_data.txt' 
INTO TABLE loyalty_program;