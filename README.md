# Project_holiday

This is for course EECS6893 final project.

We implemented a web application to provide easy search for travel blogs based on location and topic.

There are 3 parts of codes:
* FrontEnd - contains all angular JS code, css, elasticsearch code
* WebScraper - contains java codes used to crawl blogs from various travel blog websites, remove stop words, save in hadoop
* Clustering - contains java codes used to read clustering results and blog meta data, generate the data loaded into elasticsearch. It also contains a kmeans-cluster.sh script used to run mahout k-means clustering.

<b>How to set up java codes</b>
To compile WebScraper java files, you need to include the following jar files in your build path (you can download from jar directory):
* json-20160810.jar
* jsoup-1.9.2.jar
* mahout-example-0.12.2-job.jar
* Apache HttpClient 4.5.2

To compile Clustering java files, you need to include the following jar file in your build path (you can download from jar directory):
* json-20160810.jar
* mahout-example-0.12.2-job.jar

We save blog data in hdfs, so you need to start hadoop before running. We have "/opt/hadoop-2.7.3" as hadoop home path, you need to change it to the hadoop path used in your environment in WebScraper and Clustering java codes.

Our codes are tested in ubuntu environment only.

<b>How to run kmeans-cluster.sh</b>
 

Please follow https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html to set up elasticsearch. After that, you need to create index called "topicdata". Then you can run elasticsearchLoadData.sh to load data.

Now you are ready to run FrontEnd code.


