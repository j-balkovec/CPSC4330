from pyspark import SparkContext
sc = SparkContext()

# 1. RDD Transformations & Actions

# 📌 Problem: Given an RDD of movie ratings in the format (user_id, movie_id, rating), perform the following operations:
# 	•	Filter ratings greater than or equal to 4
# 	•	Map data to (movie_id, rating) format
# 	•	Reduce by key to get total ratings per movie
# 	•	Sort the movies by their total rating count in descending order

# Sample Data Set
ratings = [
    (1, "Inception", 5),
    (2, "Titanic", 3),
    (3, "Inception", 4),
    (4, "Avatar", 5),
    (5, "Titanic", 4),
    (6, "Inception", 5)
]


rdd = sc.parallelize(ratings)

ans1 = rdd.filter(lambda x: x[2] >= 4)
ans2 = rdd.map(lambda x: (x[1], 1)) # ("name", rating)
ans2 = rdd.reduceByKey(lambda a, b: a + b)
ans3 = rdd.sortBy(lambda x: x[1], ascending=False)


# 2. Word Count with RDD

# 📌 Problem: Implement a word count program using RDDs to count the occurrences of each word in a given text file.
# 	•	Convert each line into words
# 	•	Filter out stopwords like “the”, “is”, “and”, etc.
# 	•	Map words to key-value pairs (word, 1)
# 	•	Reduce by key to compute final word count

# Sample Data Set
text = ["the spark is fast but hadoop is slow"]

rdd = sc.parallelize(text)
words = rdd.flatMap(lambda line: line.split(" "))

stopwords = {"the", "is", "and"}
filtered = words.filter(lambda word: word not in stopwords)

mapped = rdd.map(lambda x: (x, 1))
reduced = rdd.reduceByKey(lambda a,b: a + b)


# Spark DataFrame Practice Problems

# 3. Data Cleaning & Transformations

# 📌 Problem: Given a DataFrame containing customer orders, perform the following operations:
# 	•	Remove rows where order_amount is NULL
# 	•	Create a new column discounted_price as order_amount * 0.9 if the category is “electronics”
# 	•	Find the total number of orders per customer

# Sample Data Set
orders = [
    (1, "Laptop", "electronics", 1000),
    (2, "Shoes", "fashion", 200),
    (3, "TV", "electronics", 1500),
    (4, "Headphones", "electronics", 100),
    (5, "Shoes", "fashion", None)
]

columns = ["customer_id", "product", "category", "order_amount"]
df = sc.createDataFrame(orders, columns)

df_cleaned = df.filter(col("order_amount").isNotNull())
df_discounted = df_cleaned.withColumn(
    "discounted_price", 
    when(col("category") == "electronics", col("order_amount") * 0.9).otherwise(col("order_amount"))
)

df_orders_count = df_discounted.groupBy("customer_id").agg(count("*").alias("total_orders"))

# 4. Joins & Aggregation

# 📌 Problem: Given two DataFrames, customers and transactions, perform:
# 	•	Join them on customer_id
# 	•	Compute the total amount spent by each customer
# 	•	Find the customer with the highest total spend

# Sample Data Set
customers = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
transactions = [(1, "Laptop", 1000), (2, "Shoes", 200), (1, "Phone", 800), (3, "TV", 1500)]

customers_df = sc.createDataFrame(customers, ["customer_id", "name"])
transactions_df = sc.createDataFrame(transactions, ["transaction_id", "purchase", "amount"])

joined = customers_df.join(transactions_df, on="customer_id", how="inner")

total_spent_df = joined.groupBy("customer_id", "name").agg(sum("amount").alias("total_spent"))

top_spender_df = total_spent_df.orderBy(desc("total_spent")).limit(1)


# Hive Practice Problems

# 5. Querying Hive Tables

# 📌 Problem: You have a Hive table sales with the following schema:

CREATE TABLE sales (
    order_id INT,
    customer_id INT,
    amount FLOAT,
    order_date STRING
) PARTITIONED BY (year INT);

# Write HiveQL queries to:
# 	•	Find the total sales per year
# 	•	Retrieve orders where amount > 500
# 	•	Find the customer who made the highest purchase

SELECT year, SUM(amount) AS total_sales
FROM sales
GROUP BY year
ORDER BY year ASC;

SELECT * FROM sales WHERE amount > 500;

SELECT * FROM sales GROUP BY customer_id ORDER BY amount DESC LIMIT 1;