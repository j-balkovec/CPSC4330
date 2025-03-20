from pyspark import SparkContext

sc = SparkContext()

sc._jsc.hadoopConfiguration().get("fs.defaultFS")