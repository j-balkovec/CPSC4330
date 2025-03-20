# Jakob Balkovec
# CPSC 4330
#     Mar 4th 2025
#
# plot.py  [alias: hw4.py]
#   Data visualization for the K-Means algorithm
#
# To run:
#   >>> python plot.py
#   

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans

# Constants
FILE_NAME = r"/Users/jbalkovec/Desktop/CPSC4330/HW4/devicestatus-clean/devicestatus-mini.txt"
NUM_CLUSTERS = 5  # Adjust as needed

def parse_line(line):
    """
    Parses a line from the dataset and extracts latitude and longitude.
    
    Parameters:
        line (str): A single line from the dataset.
    
    Returns:
        tuple: (latitude, longitude) if valid, otherwise None.
    """
    try:
        parts = line.strip().split(",")
        if len(parts) < 5:
            return None

        lat, lon = float(parts[3]), float(parts[4])

        # Ignore invalid coordinates (0.0, 0.0)
        if lat == 0.0 and lon == 0.0:
            return None
        
        return (lat, lon)
    
    except ValueError as e:
        print(f"Error parsing line: {line.strip()} - {e}")
        return None

def load_coordinates(file_path):
    """
    Reads and parses the dataset to extract valid latitude-longitude pairs.
    
    Parameters:
        file_path (str): Path to the dataset file.
    
    Returns:
        pd.DataFrame: DataFrame containing latitude and longitude columns.
    """
    with open(file_path, 'r') as f:
        lines = f.readlines()
        coordinates = [parse_line(x) for x in lines if parse_line(x) is not None]

    return pd.DataFrame(coordinates, columns=["Latitude", "Longitude"])

def perform_kmeans(data, num_clusters=NUM_CLUSTERS):
    """
    Performs K-means clustering on the given dataset.
    
    Parameters:
        data (pd.DataFrame): DataFrame containing 'Latitude' and 'Longitude'.
        num_clusters (int): Number of clusters for K-means.
    
    Returns:
        tuple: (Updated DataFrame with cluster labels, cluster centroids)
    """
    kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)
    data["Cluster"] = kmeans.fit_predict(data[["Latitude", "Longitude"]])
    return data, kmeans.cluster_centers_

def plot_clusters(data, centroids, num_clusters=NUM_CLUSTERS):
    """
    Plots clustered points along with centroids.
    
    Parameters:
        data (pd.DataFrame): DataFrame containing clustered location data.
        centroids (ndarray): Coordinates of cluster centroids.
        num_clusters (int): Number of clusters.
    """
    plt.figure(figsize=(10, 7))
    sns.scatterplot(x="Latitude", y="Longitude", hue="Cluster", palette="tab10", data=data, alpha=0.7, edgecolor="k")
    plt.scatter(centroids[:, 0], centroids[:, 1], color="red", marker="X", s=200, label="Centroids")
    
    plt.xlabel("Latitude", fontsize=12)
    plt.ylabel("Longitude", fontsize=12)
    plt.title(f"K-Means Clustering of Device Locations (K={num_clusters})", fontsize=14, fontweight="bold")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.show()

# Main Execution
if __name__ == "__main__":
    coordinates_df = load_coordinates(FILE_NAME)
    clustered_data, centroids = perform_kmeans(coordinates_df)
    plot_clusters(clustered_data, centroids)