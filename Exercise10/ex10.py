from pyspark import SparkContext

sc = SparkContext(appName="WebLogAnalysis")

# --- Load logs ---
logs = sc.textFile("hdfs:///loudacre/weblogs/*")

# --- Function to handle bad input ---
def safe_parse(line):
    parts = line.split()
    if len(parts) < 3:
        return None  # Ignore invalid lines
    return parts[2], 1  # (user_id, 1)

# --- User request counts ---
user_requests = logs.map(safe_parse).filter(lambda x: x is not None)
user_request_counts = user_requests.reduceByKey(lambda a, b: a + b)

# Save user request counts
user_request_counts.map(lambda x: f"{x[0]} {x[1]}").saveAsTextFile("output/user_request_counts")

# --- Visit frequencies ---
visit_frequencies = user_request_counts.map(lambda x: (x[1], 1))
frequency_count = visit_frequencies.reduceByKey(lambda a, b: a + b)

# Save visit frequencies
frequency_count.map(lambda x: f"Users who visited {x[0]} times: {x[1]}").saveAsTextFile("output/visit_frequencies")

# --- User IPs ---
user_ips = logs.map(lambda line: (line.split()[2], line.split()[0]))
user_ips_grouped = user_ips.distinct().groupByKey()

# Save user IPs
user_ips_grouped.map(lambda x: f"{x[0]}: {', '.join(x[1])}").saveAsTextFile("output/user_ips")

print("Processing complete. Results saved in 'output/'")