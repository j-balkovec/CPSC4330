USE dualcore;

SELECT order_date 
FROM orders 
WHERE cust_id = (
    SELECT cust_id FROM customers 
    WHERE fname = 'Bridget' 
    AND city = 'Kansas City'
)
AND order_id IN (
    SELECT order_id FROM order_details 
    WHERE prod_id = 1274348
)
AND order_date BETWEEN '2013-05-01' AND '2013-05-31';