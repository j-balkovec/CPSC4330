# Jakob Balkovec
# CPSC 4330
#     Mar 4th 2025
#
# k-means.py  [alias: hw4.py]
#   K-Means algorithm as a Spark Application
#
# To run:
#   spark-submit k-means.py /hw4/devicestatus-clean-merged /hw4/k-means-out    -- [run on individual file]
#   spark-submit k-means.py /hw4/cleaned /hw4/k-means-out                      -- [run over a dir]

"""__imports___"""
from pyspark import SparkContext
import sys

"""__params__"""
# desc: default parameters for the K-Means algorithm (slides from Friday March 1st)
default_params = {"k": 5, 
                  "converge_dist": 0.1,
                  "max_iterations": 100,
                  "random_seed": 34}


# desc: prints the centroids in a formatted way, because the json module was buns
# pre:
#     centroids is a list of tuples.
# post:
#     prints the centroids in the following format:
#     [(lat1, lon1),
#      (lat2, lon2),
#      ...
#      (lat_n, lon_n)]
def print_formatted(type, centroids):
  formatted_str = "\t[\n" + ",\n".join([f"\t\t({c[0]}, {c[1]})" for c in centroids]) + "\n\t]"
  
  print("{\n\t" + f"-- {type} centroids --" + "\n")
  print(formatted_str)
  print("\n}\n")
  
  
# desc: parses a line from the dataset to extract latitude and longitude values.
#              It splits the line by commas, and returns the 
#              coordinates as a tuple (latitude, longitude).
# pre: 
#     the input line is a string formatted as comma-separated values.
#     the line must contain at least 5 elements after splitting by commas
# post
#     returns a tuple (latitude, longitude) if valid; otherwise returns None.
def parse_line(line):
  try:
      parts = line.split(",")
      if len(parts) < 5:
          return None
      
      # lat -> latitude  | parts[3]
      # lon -> longitude | parts[4]
      lat, lon = float(parts[3]), float(parts[4])
      
      # should never hit due to pre-processing, but just in case
      if lat == 0.0 and lon == 0.0:
          return None
        
      return (lat, lon)
  
  except Exception as e:
      print(f"Error parsing line {line}: {e}")
      return None
    
    
    
# Formula: -- [yoinked from: https://en.wikipedia.org/wiki/K-means_clustering]
#
#     << C_i = argmin_{C_j} ||x - μ_j||^2 >>
# where:
#   C_i is the centroid assigned to point x
#   C_j represents each of the k centroids
#   μ_j is the position of centroid C_j
#   ||x - μ_j||^2 is the squared Euclidean distance between point x and centroid μ_j
def closest_centroid(point, centroids):  
    return min(centroids, key=lambda c: (c[0] - point[0])**2 + (c[1] - point[1])**2)



# desc: performs the k-means clustering algorithm to find centroids based on input data.
# pre:
#     the input sc is a valid SparkContext.
#     the input_path contains data to be clustered (latitude and longitude).
# post:
#     returns the final centroids as a list.
#     saves the centroids to the `output_path`.
def kmeans(sc, 
           input_path, 
           output_path, 
           k=default_params["k"], 
           converge_dist=default_params["converge_dist"], 
           max_iterations=default_params["max_iterations"]):
    
    data = sc.textFile(input_path)
    parsed_data = data.map(parse_line).filter(lambda x: x is not None).persist() # cache
    
    centroids = parsed_data.takeSample(False, k, default_params["random_seed"]) # use the call from the slides | k = 5
    
    print_formatted("initial", centroids) # for readability
    
    movement = float("inf")
    iterations = 0
    
    while movement > converge_dist and iterations < max_iterations:
        
        clusters = parsed_data.map(lambda p: (closest_centroid(p, centroids), (p, 1)))
        
        new_centroids = (clusters
                         .reduceByKey(lambda x, y: 
                           (
                             (x[0][0] + y[0][0], x[0][1] + y[0][1]),
                             x[1] + y[1])
                           )
                         .mapValues(lambda v: (v[0][0] / v[1] if v[1] > 0 else v[0][0], v[0][1] / v[1] if v[1] > 0 else v[0][1]))
                         .map(lambda x: x[1])
                         .collect())
        
        new_centroids_with_old = list(zip(centroids, new_centroids))
        new_centroids = [nc for _, nc in new_centroids_with_old]

        movement = sum((oc[0] - nc[0])**2 + (oc[1] - nc[1])**2 for oc, nc in new_centroids_with_old)
        
        iterations += 1 # increment
        centroids = new_centroids

    sc.parallelize(centroids).coalesce(1).saveAsTextFile(output_path)
    
    # updated
    print_formatted("final", centroids) # for readability
    return centroids


# desc: main entry point for the algorithm.
if __name__ == "__main__":

  if len(sys.argv) != 3:
      print("-- usage: spark-submit k-means.py <input_path> <output_path> --")
      sys.exit(1)

  input_path = sys.argv[1]
  output_path = sys.argv[2]

  sc = SparkContext(appName="KMeans")
  
  try:
    kmeans(sc, input_path, output_path)
  except ValueError as e: # propagate everything else
    print(f"-- [error]: {e}")
  
  sc.stop()