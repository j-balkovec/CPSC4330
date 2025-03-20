USE dualcore;

SELECT state, COUNT(*) AS num_customers 
FROM customers 
GROUP BY state 
ORDER BY num_customers DESC 
LIMIT 10;