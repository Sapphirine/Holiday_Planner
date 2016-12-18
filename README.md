# Project_holiday

This is for course EECS6893 final project.

We implemented a web application to provide easy search for travel blogs based on location and topic.

There are 3 parts of codes:
* FrontEnd - contains all angular JS code, css, elasticsearch code
* WebScraper - contains java codes used to crawl blogs from various travel blog websites, remove stop words, save in hadoop
* Clustering - contains java codes used to read clustering results and blog meta data, generate the data loaded into elasticsearch. It also contains a kmeans-cluster.sh script used to run mahout k-means clustering.


