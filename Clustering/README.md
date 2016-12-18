ProcessJson.jave will read clusterdump, clustedPoints, and blog meta files, combine all information, generate a data script to load data into elasticserch

kmeans-cluster.sh is the script to run mahout k-means clustering. It takes "continent", "country", "state' as input. The input needs to match the data directory names saved in hdfs.
