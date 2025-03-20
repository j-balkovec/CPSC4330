# Jakob Balkovec
# CPSC 4330
#     Mar 4th 2025
#
# plot.R  [alias: hw4.R]
#   Data visualization for the K-Means algorithm
#
# To run:
#   >>> source("plot.R")

library(ggplot2)
library(dplyr)

# Assuming `device_status` is a dataframe with columns V4 (lat) and V5 (long)
coordinates <- device_status %>%
  select(lat = V4, long = V5) %>%
  filter(lat != 0 & long != 0)  # Filter out locations with lat/long equal to 0

# K-means parameters
K <- 5
convergeDist <- 0.1

# Randomly select K points as the initial centroids
set.seed(34)  # For reproducibility
initial_centroids <- coordinates[sample(1:nrow(coordinates), K), ]

# Function to calculate Euclidean distance
euclidean_distance <- function(p1, p2) {
  sqrt(sum((p1 - p2)^2))
}

# Function to assign clusters
assign_clusters <- function(data, centroids) {
  clusters <- vector("list", length = K)
  for (i in 1:nrow(data)) {
    distances <- apply(centroids, 1, function(centroid) euclidean_distance(data[i,], centroid))
    cluster <- which.min(distances)
    clusters[[cluster]] <- append(clusters[[cluster]], i)
  }
  return(clusters)
}

# Function to update centroids
update_centroids <- function(data, clusters) {
  new_centroids <- matrix(NA, nrow = K, ncol = 2)
  for (i in 1:K) {
    cluster_data <- data[clusters[[i]], ]
    new_centroids[i, ] <- colMeans(cluster_data)
  }
  return(new_centroids)
}

# K-means algorithm
centroids <- as.matrix(initial_centroids)
converged <- FALSE
while (!converged) {
  # Assign points to the nearest centroid
  clusters <- assign_clusters(coordinates, centroids)
  
  # Update centroids
  new_centroids <- update_centroids(coordinates, clusters)
  
  # Check for convergence
  dist_movement <- sum(apply(new_centroids - centroids, 1, function(x) sum(x^2)))
  if (dist_movement < convergeDist) {
    converged <- TRUE
  }
  
  # Update centroids for the next iteration
  centroids <- new_centroids
}

# Print the final centroids
print("Final Centroids:")
print(centroids)

# Plot the data and centroids
coordinates$cluster <- factor(unlist(lapply(1:K, function(i) rep(i, length(clusters[[i]])))))
ggplot(coordinates, aes(x = lat, y = long, color = cluster)) +
  geom_point(alpha = 0.6) +
  geom_point(data = as.data.frame(centroids), aes(x = V1, y = V2), color = "red", size = 5, shape = 4) +
  theme_minimal() +
  ggtitle("K-Means Clustering of Device Locations") +
  xlab("Latitude") +
  ylab("Longitude")