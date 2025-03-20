# Jakob Balkovec
# CPSC 4330
#     Feb 24th 2025
#
# hw3_1.py
# Spark application for part1 of HW3
#
# To run:
#   spark-submit hw3_1.py /hw3/review_data/

"""__imports__"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,
                                   count,
                                   avg)

import sys, os

# get file handle    
if len(sys.argv) != 2:
    print("-- usage: spark-submit hw3_1.py <input_file> --")
    sys.exit(1)

FILE_HANDLE = sys.argv[1]

if not os.path.exists(FILE_HANDLE):
    print(f"-- file '{FILE_HANDLE}' not found. Please provide a valid path --")
    sys.exit(1)
    
    
# init spark session
spark = SparkSession.builder.appName("product-review-analysis").getOrCreate()

# load CSV file
df = spark.read.csv(FILE_HANDLE, header=False, inferSchema=True) # FIXME: pass filename as arg

# rename for clarity
df = df.withColumnRenamed("_c0", "product_id") \
       .withColumnRenamed("_c1", "irrelevant") \
       .withColumnRenamed("_c2", "rating") \
       .withColumnRenamed("_c3", "also-irrelevant") \

# drop unnecessary columns
df = df.select("product_id", "rating") 

# cache
df.cache()
 
# compute total reviews and average per product
result = df.groupBy("product_id") \
           .agg(count("rating").alias("total_reviews"), avg("rating").alias("avg_rating")) \
           .orderBy("product_id")
           
# show results
# result.show()

# single partition
result.write.csv("part-1", header=True)
       
spark.stop()