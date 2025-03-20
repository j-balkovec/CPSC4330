# CPSC 4330 Big Data Analytics - Homework 5

**Due: 10:55 AM, Monday, March 17**

---

## Problem 1

Dualcore recently started a loyalty program to reward their best customers. They have provided a sample data file (`loyalty_data.txt`) containing information about customers who signed up for the program.

### Task 1
Write a HiveQL query (`hw5_1.hql`) to create the `loyalty_program` table.

**Query:**  
```sql
USE dualcore;

CREATE TABLE loyalty_program (
    customer_id INT,
    first_name STRING,
    email STRING,
    last_name STRING,
    loyalty_level STRING,
    phone_numbers MAP<STRING, STRING>,
    order_ids ARRAY<INT>,
    order_summary STRUCT<
        min:DOUBLE,
        max:DOUBLE,
        avg:DOUBLE,
        total:DOUBLE>
)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;
```

**Answer:**  
```
Tables:

customers    
order_details 
orders       
products     
loyalty_program 
```

---

### Task 2
Write a query (`hw5_2.hql`) to load the data from `loyalty_data.txt` into Hive.

**Query:**  
```sql
USE dualcore;

LOAD DATA INPATH '/hw5/loyalty_data.txt' 
INTO TABLE loyalty_program;
```

**Answer:**  
```
[LIMIT 5]

2025-03-10 21:16:37 1000238     Christy Herrin  christy.herrin@example.com      {"SILVER":null} [null]  {"min":5179798.0,"max":5346469.0,"avg":5517663.0,"total":5783754.0}
2025-03-10 21:16:37 1000279     Casey   Franco  franco81@example.com    {"SILVER":null} [null]  {"min":5262426.0,"max":5307405.0,"avg":5477142.0,"total":5507578.0}
2025-03-10 21:16:37 1000810     Adam    Montoya amontoya@example.com    {"SILVER":null} [null]  {"min":5006384.0,"max":5057993.0,"avg":5220993.0,"total":5401325.0}
2025-03-10 21:16:37 1001219     Ervin   Groff   eg1981@example.com      {"SILVER":null} [null]  {"min":5104660.0,"max":5287498.0,"avg":5679336.0,"total":5790583.0}
2025-03-10 21:16:37 1001661     Jody    Culver  jody.culver@example.com {"SILVER":null} [null,null]     {"min":5002071.0,"max":5132660.0,"avg":5459665.0,"total":5478462.0}
```

---

### Task 3
Write a query (`hw5_3.hql`) to select the HOME phone number for customer ID `1200866`.

**Query:**  
```sql
USE dualcore;

SELECT phone_numbers['HOME'] 
FROM loyalty_program 
WHERE customer_id = 1200866;
```

**Answer:**  
```
408-555-4914
```

---

### Task 4
Write a query (`hw5_4.hql`) to select the third element from the `order_ids` for customer ID `1200866`.

**Query:**  
```sql
USE dualcore;

SELECT order_ids[2] /* 0-indexed */
FROM loyalty_program 
WHERE customer_id = 1200866;
```

**Answer:**  
```
5278505
```

---

## Problem 2

This problem involves the `customers`, `orders`, `order_details`, and `products` tables from exercise 13.

### Task 5
Write a HiveQL query (`hw5_5.hql`) to find how many products have been bought by customer `1071189`.

**Query:**  
```sql
USE dualcore;

SELECT COUNT(*) 
FROM order_details 
WHERE order_id IN (
    SELECT order_id FROM orders WHERE cust_id = 1071189
);
```

**Answer:**  
```
20
```

---

### Task 6
Write a HiveQL query (`hw5_6.hql`) to find how many customers have spent more than `300000` on the total price of all products they have bought.

**Query:**  
```sql
USE dualcore;

SELECT COUNT(*) FROM (
  SELECT o.cust_id, SUM(p.price)
  FROM order_details d
  JOIN orders o ON (o.order_id = d.order_id)
  JOIN products p ON (d.prod_id = p.prod_id)
  GROUP BY o.cust_id
  HAVING SUM(p.price) > 300000
) spent_a_lot;
```

**Answer:**  
```
16852
```

---

### Task 7
Write a HiveQL query (`hw5_7.hql`) to list customers (`cust_id` only) who have not placed any order.

**Query:**  
```sql
USE dualcore;

SELECT c.cust_id
FROM customers c
LEFT JOIN orders o
    ON c.cust_id = o.cust_id
WHERE o.order_id IS NULL;
```

**Answer:**  
```
[HEAD]

1124975
1124977
1125034
1125039
1125056
1125057
1125085
1125093
```

---

## Problem 3

This problem involves the `ratings` table inside the `dualcore` database.

### Task 8
Write a HiveQL query (`hw5_8.hql`) to find the product with the lowest average rating among products with at least `50` ratings.

**Query:**  
```sql
USE dualcore;

SELECT prod_id, AVG(rating) AS average 
FROM ratings 
GROUP BY prod_id 
HAVING COUNT(*) > 50 
ORDER BY average ASC 
LIMIT 1;
```

**Answer:**  
```
1274673    1.1025260029717683
```

---

### Task 9
Write a HiveQL query (`hw5_9.hql`) to find the five most common trigrams (three-word combinations) in the comments for the product identified in Task 8.

**Query:**  
```sql
USE dualcore;

/* prod_id from q1*/
SELECT EXPLODE(NGRAMS(SENTENCES(LOWER(message)), 3, 5)) 
FROM ratings 
WHERE prod_id = 1274673;
```

**Answer:**  
```
{"ngram":["cost","ten","times"],"estfrequency":71.0}
{"ngram":["does","the","red"],"estfrequency":71.0}
{"ngram":["ten","times","more"],"estfrequency":71.0}
{"ngram":["than","the","others"],"estfrequency":71.0}
{"ngram":["the","red","one"],"estfrequency":71.0}
```

---

### Task 10
Write a HiveQL query (`hw5_10.hql`) to list the comments that contain the phrase `"ten times more"` for the product identified in Task 8.

**Query:**  
```sql
USE dualcore;

/** prod_id from q1*/
SELECT message 
FROM ratings
WHERE prod_id = 1274673 
AND message LIKE "%ten times more%";
```

**Answer:**  
```
Why does the red one cost ten times more than the others?
```

---