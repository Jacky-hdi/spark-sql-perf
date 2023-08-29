# Spark SQL Performance Tests for HDInsight


# TPCDS

## Setup a benchmark

Before running any query, a dataset needs to be setup by creating a `Benchmark` object. Generating
the TPCDS data requires dsdgen built and available on the machines. We have a fork of dsdgen that
you will need. The fork includes changes to generate TPCDS data to stdout, so that this library can
pipe them directly to Spark, without intermediate files. Therefore, this library will not work with
the vanilla TPCDS kit.

TPCDS kit needs to be installed on all cluster executor nodes under the same path!

It can be found [here](https://github.com/databricks/tpcds-kit).

```
// Generate the data and create external tables in HiveMetaStore
spark-submit --class com.databricks.spark.sql.perf.tpcds.GenTPCDSData --master yarn --num-executors 8 --driver-memory 8G --executor-memory 4G --executor-cores 2 spark-sql-perf-assembly-0.7.1-SNAPSHOT.jar -m yarn -d /tmp -s 10 -l "abfs:///tpcds10" -f parquet --databaseName tpcds10
```



## Run benchmarking queries
After setup, users can use `runExperiment` function to run benchmarking queries and record query execution time. Taking TPC-DS as an example, you can start an experiment by using

```
spark-submit --class com.databricks.spark.sql.perf.tpcds.RunTPCDSBenchmark --master yarn --num-executors 8 --driver-memory 8G --executor-memory 4G --executor-cores 2 spark-sql-perf-assembly-0.7.1-SNAPSHOT.jar -m yarn -l /benchmark/result --databaseName tpcds10
```

By default, experiment will be started in a background thread.
For every experiment run (i.e. every call of `runExperiment`), Spark SQL Perf will use the timestamp of the start time to identify this experiment. Performance results will be stored in the sub-dir named by the timestamp in the given `spark.sql.perf.results` (for example `/tmp/results/timestamp=1429213883272`). The performance results are stored in the JSON format.

## Retrieve results
While the experiment is running you can use `experiment.html` to get a summary, or `experiment.getCurrentResults` to get complete current results.
Once the experiment is complete, you can still access `experiment.getCurrentResults`, or you can load the results from disk.

```
// Get all experiments results.
val resultTable = spark.read.json(resultLocation)
resultTable.createOrReplaceTempView("sqlPerformance")
sqlContext.table("sqlPerformance")
// Get the result of a particular run by specifying the timestamp of that run.
sqlContext.table("sqlPerformance").filter("timestamp = 1429132621024")
// or
val specificResultTable = spark.read.json(experiment.resultPath)
```

You can get a basic summary by running:
```
experiment.getCurrentResults // or: spark.read.json(resultLocation).filter("timestamp = 1429132621024")
  .withColumn("Name", substring(col("name"), 2, 100))
  .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
  .select('Name, 'Runtime)
```