# Jakob Balkovec
# CPSC 4330
#     Feb 24th 2025
#
# hw3_2.py
# Spark application for part2 of HW3
#
# ----------------------------------------------------------------------------------------
#  <!--- Copied from hw2_2.ipynb --->
#
# The file College_2015_16.csv (posted on Canvas) contains the following fields:
#
# - Unique ID
# - Name
# - City
# - State
# - Zip
# - Admission rate
# - Average SAT score
# - Enrollment
# - CostA
# - CostP
#
# The last two columns are the cost of public and private universities. 
# If one is non-null, the other should be null. If both are null, 
# that's a missing value. If both are non-null, use either value
# ----------------------------------------------------------------------------------------
# To Run:
#     spark-submit hw3_2.py /hw3/College_2015_16.csv /hw3/College_2017_18.csv
#
# pyspark                   3.5.4              pyhd8ed1ab_0         --- [local]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

import pyspark

import json

import sys
import os

print("pyspark --v: " + pyspark.__version__ + "\n") # --- [local]

# ---------------------- FILE PATHS ----------------------
# get file handle
if len(sys.argv) != 3:
    print("-- usage: spark-submit hw3_2.py <COLLEGE_2015> <COLLEGE_2017> --")
    sys.exit(1)

COLLEGE_2015 = sys.argv[1]
COLLEGE_2017 = sys.argv[2]

if not os.path.exists(COLLEGE_2015):
    print(f"-- file '{COLLEGE_2015}' not found. Please provide a valid path --")
    sys.exit(1)

if not os.path.exists(COLLEGE_2017):
    print(f"-- file '{COLLEGE_2017}' not found. Please provide a valid path --")
    sys.exit(1)
        
# ---------------------- FILE PATHS ----------------------


# ---------------------- SCHEMA ----------------------
# Line: 100654 (ID),Alabama A & M University (Name),
#       Normal(City),AL(State),35762(ZIP),0.9027(Rate),
#       929(SAT),4824(Enrollment),22886(CostA),NULL(CostP)

schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("city", StringType(), True),
  StructField("state", StringType(), True),
  StructField("zip", StringType(), True),
  StructField("admissionRate", DoubleType(), True),
  StructField("averageSAT", IntegerType(), True),
  StructField("enrollment", IntegerType(), True),
  StructField("costA", IntegerType(), True),
  StructField("costP", IntegerType(), True)
])
# ---------------------- SCHEMA ----------------------


# ------------------- SPARK CONFIG -------------------
spark = SparkSession.builder \
                    .appName("college-data-analysis") \
                    .master("local[*]") \
                    .getOrCreate()

# to see errors in the console
spark.sparkContext.setLogLevel("INFO")

# load data
df_2015 = spark.read.csv(COLLEGE_2015, header=False, schema=schema)
# ------------------- SPARK CONFIG -------------------

# ------------------------ Q1 ------------------------
# Convert the lines in the file to a tuple of fields, 
# and only keep these attributes: ID, name, state, enrollment, 
# and cost, where cost is either costA or costP as above. 
# If enrollment cannot be converted to an int, set it to null.

# filter the data
df_2015_filtered = df_2015.select(
    col("id"),
    col("name"),
    col("state"),
    when(col("enrollment").isNotNull(), col("enrollment")).otherwise(None).alias("enrollment"),
    when(col("costA").isNotNull(), col("costA")).otherwise(col("costP")).alias("cost")
    # TODO: filter out the entries where both prices are NULL
)

print("# ------------------------ Q1 ------------------------\n")
print("\t-- data-frame:", df_2015_filtered, "\n")
print("# ----------------------------------------------------\n\n")

df_2015_filtered.cache()
# ------------------------ Q1 ------------------------



# ------------------------ Q2 ------------------------
# Find how many records were filtered due to the invalid number 
# of fields in the data (the file has 10 fields).

total_records = df_2015.count()
valid_records = df_2015_filtered.count()
filtered_records = total_records - valid_records

print("# ------------------------ Q2 ------------------------\n")
print("\t-- records filtered due to invalid fields:", filtered_records, "\n")
print("# ----------------------------------------------------\n\n")
# ------------------------ Q2 ------------------------



# ------------------------ Q3 ------------------------
# Find how many records are there from the state of California?

ca_count = df_2015_filtered.filter(col("state") == "CA").count()

print("# ------------------------ Q3 ------------------------\n")
print("\t\t-- number of records from CA:", ca_count, "\n")
print("# ----------------------------------------------------\n\n")
# ------------------------ Q3 ------------------------



# ------------------------ Q4 ------------------------
# What percentage of the records have a non-null enrollment?

total_valid_enrollment = df_2015_filtered.filter(col("enrollment").isNotNull()).count()
percentage_enrollment = (total_valid_enrollment / valid_records) * 100

print("# ------------------------ Q4 ------------------------\n")
print("\t\t-- percentage of records with non-null enrollment:", round(percentage_enrollment, ndigits=2), "%\n")
print("# ----------------------------------------------------\n\n")
# ------------------------ Q4 ------------------------



# ------------------------ Q5 ------------------------
# What is the name and cost of the 5 most expensive universities?
print("# ------------------------ Q5 ------------------------\n")
df_2015_filtered.orderBy(desc("cost")).select("name", "cost").show(5)
print("# ----------------------------------------------------\n\n")
# ------------------------ Q5 ------------------------



# ------------------------ Q6 ------------------------
# Find the number of universities in each state.
print("# ------------------------ Q6 ------------------------\n")
df_2015_filtered.groupBy("state").count().orderBy("state").show(n=df_2015_filtered.count(), truncate=False) # 50 states + others
print("# ----------------------------------------------------\n\n")
# ------------------------ Q6 ------------------------



# ------------------------ Q7 ------------------------
# Find the total number of enrollments in each state.
print("# ------------------------ Q7 ------------------------\n")
df_2015_filtered.groupBy("state").sum("enrollment").orderBy("state").show(n=df_2015_filtered.count(), truncate=False) # 50 states + others
print("# ----------------------------------------------------\n\n")
# ------------------------ Q7 ------------------------



# ------------------------ Q8 ------------------------
# Find the average enrollment for each state.
print("# ------------------------ Q8 ------------------------\n")
df_2015_filtered.groupBy("state").agg(avg("enrollment")).orderBy("state").show(n=df_2015_filtered.count(), truncate=False) # 50 states + others
print("# ----------------------------------------------------\n\n")
# ------------------------ Q8 ------------------------



# ------------------------ Q9 ------------------------
# Another file “College_2017_18.csv” (posted on Canvas) has the 
# college enrollment data of year 2017 - 2018. Write code to 
# calculate percent of change in enrollment from year 2015 – 2016 
# to the year 2017 – 2018

df_2017 = spark.read.csv(COLLEGE_2017, schema=schema, header=False)
df_2017_filtered = df_2017.select(
    col("id"),
    col("state"),
    when(col("enrollment").isNotNull(), col("enrollment")).otherwise(None).alias("enrollment-2017"),
    when(col("costA").isNotNull(), col("costA")).otherwise(col("costP")).alias("cost")
    # TODO: filter out the entries where both prices are NULL
)

df_2017_filtered.cache()

# join on ID
df_joined = df_2015_filtered.join(df_2017_filtered, "id", "inner")

# compute percent change
df_change = df_joined.withColumn("percent-change", ((col("enrollment-2017") - col("enrollment")) / col("enrollment")) * 100)

print("# ------------------------ Q9 ------------------------\n")
df_change.select("id", "enrollment", "enrollment-2017", "percent-change").show()
print("# ----------------------------------------------------\n\n")
# ------------------------ Q9 ------------------------