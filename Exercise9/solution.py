from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, count, desc

# Initialize Spark Session
spark = SparkSession.builder.appName("WeblogAnalysis").getOrCreate()

# Load the dataset
log_df = spark.read.text("/path/to/weblogs.log")

# Define regex patterns to extract relevant fields
log_pattern = r'(\S+) - \S+ \[(.*?)\] "(.*?) (.*?) .*" (\d+) \d+ ".*"  "(.*?)"'

# Extract required fields
log_df = log_df.select(
    regexp_extract('value', log_pattern, 1).alias('ip'),
    regexp_extract('value', log_pattern, 2).alias('timestamp'),
    regexp_extract('value', log_pattern, 3).alias('http_method'),
    regexp_extract('value', log_pattern, 4).alias('url'),
    regexp_extract('value', log_pattern, 5).alias('response_code'),
    regexp_extract('value', log_pattern, 6).alias('browser')
)

# Count requests per HTTP method
http_method_count = log_df.groupBy("http_method").count()

# Find the top 5 most accessed URLs
top_urls = log_df.groupBy("url").count().orderBy(desc("count")).limit(5)

# Count occurrences of each HTTP response code
response_code_count = log_df.groupBy("response_code").count()

# Identify the top 3 most common browser types
top_browsers = log_df.groupBy("browser").count().orderBy(desc("count")).limit(3)

# Save results to CSV files
http_method_count.write.csv("/output/http_method_count", header=True)
top_urls.write.csv("/output/top_urls", header=True)
response_code_count.write.csv("/output/response_code_count", header=True)
top_browsers.write.csv("/output/top_browsers", header=True)

# Stop Spark Session
spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, count, desc

# Initialize Spark Session
spark = SparkSession.builder.appName("WeblogAnalysis").getOrCreate()

# Load the dataset from HDFS
log_df = spark.read.text("hdfs:///weblogs/weblogs.log")

# Define regex patterns to extract relevant fields
log_pattern = r'(\S+) - \S+ \[(.*?)\] "(.*?) (.*?) .*" (\d+) \d+ ".*"  "(.*?)"'

# Extract required fields
log_df = log_df.select(
    regexp_extract('value', log_pattern, 1).alias('ip'),
    regexp_extract('value', log_pattern, 2).alias('timestamp'),
    regexp_extract('value', log_pattern, 3).alias('http_method'),
    regexp_extract('value', log_pattern, 4).alias('url'),
    regexp_extract('value', log_pattern, 5).alias('response_code'),
    regexp_extract('value', log_pattern, 6).alias('browser')
)

# Count requests per HTTP method
http_method_count = log_df.groupBy("http_method").count()

# Find the top 5 most accessed URLs
top_urls = log_df.groupBy("url").count().orderBy(desc("count")).limit(5)

# Count occurrences of each HTTP response code
response_code_count = log_df.groupBy("response_code").count()

# Identify the top 3 most common browser types
top_browsers = log_df.groupBy("browser").count().orderBy(desc("count")).limit(3)

# Save results back to HDFS
http_method_count.write.csv("hdfs:///weblogs/output/http_method_count", header=True)
top_urls.write.csv("hdfs:///weblogs/output/top_urls", header=True)
response_code_count.write.csv("hdfs:///weblogs/output/response_code_count", header=True)
top_browsers.write.csv("hdfs:///weblogs/output/top_browsers", header=True)

# Stop Spark Session
spark.stop()
