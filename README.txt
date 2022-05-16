Spark
=====
- run in parallelism
- faster than Hadoop MapReduce

- execution plant able to identify the relationship between 2 job execution and run together.
- Worker Node -> Partition -> Task
- RDD - Resilient Distributed Dataset - Data which we working with distributed. (Abstraction)

*- When Running a JavaRDD , it doesn't really running, but building an execution plant ( sequence)
-Each JavaRDD just push into partitions as a working logic.

-DAG - Directed Acyclic Graph ( Same with Apache Airflow?) == Task
-Spark is developed by Scala

-core, sql, and hadoop ( as dependencies)
-I have a RDD, and Reduce the value into

-Mapping : Transform the data into another rdd

- Sort and Coalesce
Sort: sort and foreach not printing in order as partitioning occurs in spark (X?)
*misunderstood about the foreach (not forEach) logic.
*take - able to retrieve the accurate top X data regardless of partitions - when data is finished
*collect - when data is finished * (confident)
*We should get correct sorting regardless of how Spark organised the data.
Coalesce: Specify how many partitions to end up with.