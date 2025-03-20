/**
Jakob Balkovec
CPSC 4330
Mon Mar 10th 2025

HW5_6:

For this problem, you will need customers, orders, order_details, and products tables that you
have prepared in exercise 13.

Write a HiveQL query ‘hw5_6.hql’ to find how many customers have spent more than
300000 on the total price of all products that s/he has bought?
*/

USE dualcore;

SELECT COUNT(*) FROM (
  SELECT o.cust_id, SUM(p.price)
  FROM order_details d
  JOIN orders o ON (o.order_id = d.order_id)
  JOIN products p ON (d.prod_id = p.prod_id)
  GROUP BY o.cust_id
  HAVING SUM(p.price) > 300000
) spent_a_lot;