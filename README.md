# Spark

#Spark-HDFS-Aggregator

Two stage of Aggregration done by Spark SQL APIs

Spark reads data from HDFS table --> Performs an SQL query "SELECT CONCAT(STARTTIME,'-',ENDTIME) as TimeStamp, a as A, b as B,count(x) as xCount, COALESCE(sum(x),0) as xSum from TableName where in_sec >= STARTTIME and in_sec < ENDTIME group by a, b" --> Write back the result to HDFS using HDFS client.

Spark reads the result of Stage One aggregation --> performs a second level query --> Append the result to HDFS through hdfs client.
