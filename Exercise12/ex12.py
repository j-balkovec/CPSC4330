# Jakob Balkovec
# CPSC 4330
#     Mar 5th 2025
#
# ex12.py
# Spark application for in-class exercise 12
#
# To run:
#   spark-submit ex12.py /ex12/kddcup.data/

"""__imports__"""
from pyspark.sql import SparkSession

from pyspark.sql.functions import (count,
                                   sum)
import sys
import os

             
# get file handle                            
if len(sys.argv) != 2:
    print("-- usage: spark-submit ex12.py <input_file> --")
    sys.exit(1)

FILE_HANDLE = sys.argv[1]

if not os.path.exists(FILE_HANDLE):
    print(f"-- file '{FILE_HANDLE}' not found. Please provide a valid path --")
    sys.exit(1)                            
    

spark_session = SparkSession.builder \
                            .appName("ex-12") \
                            .getOrCreate()
                            
# read in the data
data_frame = spark_session.read.csv(FILE_HANDLE, header=False)                                                        

# rename columns
# line:
#     0,tcp,http,SF,215,45076,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,1,1,0.00,0.00,0.00,0.00,1.00,0.00,0.00,0,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,normal.
# >>> len(line.split(',')) = 42 (1 based)
data_frame = data_frame.withColumnRenamed("_c0", "duration") \
                       .withColumnRenamed("_c1", "protocol_type") \
                       .withColumnRenamed("_c41", "service")
                       
# drop unnecessary columns
data_frame = data_frame.select("duration", "protocol_type", "service")

# cache
data_frame.cache()

# --------------------
# 1. total number of connections
total_connections = data_frame.count()
print(total_connections)

# --------------------
# 2. total duration time of connections per protocol type
total_duration = data_frame.groupBy("protocol_type") \
                            .agg(sum("duration").alias("total_duration")) \
                            .orderBy("protocol_type")
total_duration.show()

# --------------------
# 3. Find out which protocol is most vulnerable to attacks. In other words, which protocol has
#    the highest number of attacks. “normal” is no attack; other values of the attribute Label
#    are considered as attack.     
attacks = data_frame.filter(data_frame["service"] != "normal") \
                    .groupBy("protocol_type") \
                    .agg(count("service").alias("total_attacks")) \
                    .orderBy("total_attacks", ascending=False)
attacks.show()                      

# --------------------
# 4. Save all as JSON file

total_duration.write.json("total_duration.json")
attacks.write.json("attacks.json")