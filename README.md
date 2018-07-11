# Spark dataframe processing module

This project is testing.

### Prerequisites

 * Scala 2.11.12
 * SBT
 * Cloudera Spark 5.14
 * Spark Core 2.3.0
 * Spark SQL 2.3.0
 * Scala Test
 
### Running applications

 1. SBT Build.
    * sbt clean package
 2. Upload created jar file to yarn cluster or your local host.
 3. Spark Submit.
    * spark-submit2 --master [yarn or local] --deploy-mode [client or cluster] --class com.hhi.sap.main.Main [path to jar file]
 4. If you need, running the test application.