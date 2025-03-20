# Exercise 9
# Spark | Hadoop
# Jakob Balkovec
# CPSC 4330

# Assignment: Analyze weblog data using PySpark
#
# 1. Load the dataset into a PySpark DataFrame.
#
# 2. Extract the following fields:
#    - Timestamp
#    - HTTP method (GET/POST)
#    - URL requested
#    - HTTP response code
#    - Browser type
#
# 3. Count the number of requests per HTTP method.
#
# 4. Find the top 5 most accessed URLs.
#
# 5. Determine the number of occurrences of each HTTP response code.
#
# 6. Identify the top 3 most common browser types.
#
# 7. Save the final results to a CSV file.
#
#
# Use PySpark DataFrame operations for processing.
# Assume the file path is '/path/to/weblogs.log'.

from pyspark.sql import SparkSession

from pyspark.sql.functions import (col, 
                                   regexp_extract, 
                                   count, 
                                   desc)

import logging
import os
import re

# --------------------- CONFIG --------------------- #

# make sure the 'logs' directory exists
log_directory = "logs"
os.makedirs(log_directory, exist_ok=True)


def setup_logger(name, log_filename):
    log_file = os.path.join(log_directory, log_filename)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # file handler
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # prevent duplicate handlers
    if not logger.handlers:
        logger.addHandler(file_handler)

    return logger

spark_logger = setup_logger('spark_session', 'spark_session_00000.log')

# --------------------- CONFIG --------------------- #

WEBLOGS_PATH = r'/loudacre/weblogs' # FIXME: Path could be wrong
spark_logger.info(f"for logs, using path: {WEBLOGS_PATH}")


spark = SparkSession.builder.appName("WeblogAnalysis").getOrCreate()
spark_logger.info(f"started spark session: {spark}")

# Fit into a data frame

try:
  log_df = spark.read.text(WEBLOGS_PATH)
except Exception as e:
  spark_logger.error(f"df error >> error: {e}")
  
spark_logger.info("loaded data frame successfully")


# Filter
#   LOG ENTRY:
#       12.13.139.87 - 96828 [16/Sep/2013:23:59:44 +0100] "GET /KBDOC-00129.html HTTP/1.0" 200 17834 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F24L"
regex: str = r'(\S+) - \S+ \[(.*?)\] "(.*?) (.*?) .*" (\d+) \d+ ".*"  "(.*?)"'

log_pattern = re.compile(regex) # to match: log_pattern == entry


log_df = log_df.select(
    regexp_extract('value', regex, 1).alias('ip'),
    regexp_extract('value', regex, 2).alias('timestamp'),
    regexp_extract('value', regex, 3).alias('http_method'),
    regexp_extract('value', regex, 4).alias('url'),
    regexp_extract('value', regex, 5).alias('response_code'),
    regexp_extract('value', regex, 6).alias('browser')
)

spark_logger.info("selected fields")

# 3. Count the number of requests per HTTP method.
number_of_http_req = log_df.groupBy("http_method").count()
spark_logger.info(f"number_of_http_req = {number_of_http_req}")

# 4. Find the top 5 most accessed URLs.
top_urls = log_df.groupBy("url").count().orderBy(desc("count")).limit(5)
spark_logger.info(f"top_urls = {top_urls}")

# 5. Determine the number of occurrences of each HTTP response code.
response_code_count = log_df.groupBy("response_code").count()
spark_logger.info(f"response_code_count = {response_code_count}")

# 6. Identify the top 3 most common browser types.
top_browsers = log_df.groupBy("browser").count().orderBy(desc("count")).limit(3)
spark_logger.info(f"top_browsers = {top_browsers}")

# 7. Save the final results to a CSV file.
number_of_http_req.write.csv("/output/number_of_http_req", header=True)
top_urls.write.csv("/output/top_urls", header=True)
response_code_count.write.csv("/output/response_code_count", header=True)
top_browsers.write.csv("/output/top_browsers", header=True)

# http_method_count.write.csv("hdfs:///weblogs/output/http_method_count", header=True)
# top_urls.write.csv("hdfs:///weblogs/output/top_urls", header=True)
# response_code_count.write.csv("hdfs:///weblogs/output/response_code_count", header=True)
# top_browsers.write.csv("hdfs:///weblogs/output/top_browsers", header=True)

spark.stop()


