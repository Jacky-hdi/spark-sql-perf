package com.databricks.spark.sql.perf.tpcds


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, explode, max, min, stddev, when}





case class RunTPCDSConfig(
                      master: String = "local[*]",
                      location: String = null,
                      databaseName: String = "tpcds",
                      filter: Option[String] = None,
                      iterations: Int = 3,
                      baseline: Option[Long] = None)

object RunTPCDSBenchmark {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunTPCDSConfig]("spark-sql-perf") {
      head("spark-sql-perf", "0.7.0")
      opt[String]('m', "master")
        .action { (x, c) => c.copy(master = x) }
        .text("the Spark master to use, default to local[*]")
      opt[String]('f', "filter")
        .action((x, c) => c.copy(filter = Some(x)))
        .text("a filter on the name of the queries to run")
      opt[String]('l', "location")
        .action((x, c) => c.copy(location = x))
        .text("root directory of location to save benchmark results")
      opt[String]("databaseName")
        .action((x, c) => c.copy(databaseName = x))
        .text("HiveMetaStore database name")
      opt[Int]('i', "iterations")
        .action((x, c) => c.copy(iterations = x))
        .text("the number of iterations to run")
      opt[Long]('c', "compare")
        .action((x, c) => c.copy(baseline = Some(x)))
        .text("the timestamp of the baseline experiment to compare with")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, RunTPCDSConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: RunTPCDSConfig): Unit = {
    val conf = new SparkConf()
      .setMaster(config.master)
      .setAppName(getClass.getName)

    val sparkSession = SparkSession.builder.config(conf).getOrCreate()
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._

    val tpcds = new TPCDS (sqlContext = sqlContext)
    val randomizeQueries = false
    def queries = {
      val filtered_queries = config.filter match {
        case None => tpcds.tpcds2_4Queries
        case _ => tpcds.tpcds2_4Queries.filter(q => config.filter.contains(q.name))
      }
      if (randomizeQueries) scala.util.Random.shuffle(filtered_queries) else filtered_queries
    }

    sqlContext.sql(s"use ${config.databaseName}")

    val experiment = tpcds.runExperiment(
      queries,
      iterations = config.iterations,
      resultLocation = config.location,
      tags = Map(
        "runtype" -> "hdinsight", "database" -> "tpcds"))

    println("== STARTING EXPERIMENT ==")
    experiment.waitForFinish(1000 * 60 * 30)

    sqlContext.setConf("spark.sql.shuffle.partitions", "1")

    val toShow = experiment.getCurrentRuns()
      .withColumn("result", explode($"results"))
      .select("result.*")
      .groupBy("name")
      .agg(
        min($"executionTime") as 'minTimeMs,
        max($"executionTime") as 'maxTimeMs,
        avg($"executionTime") as 'avgTimeMs,
        stddev($"executionTime") as 'stdDev,
        (stddev($"executionTime") / avg($"executionTime") * 100) as 'stdDevPercent)
      .orderBy("name")

    println("Showing at most 100 query results now")
    toShow.show(100)

    println(s"""Results: sqlContext.read.json("${experiment.resultPath}")""")

    config.baseline.foreach { baseTimestamp =>
      val baselineTime = when($"timestamp" === baseTimestamp, $"executionTime").otherwise(null)
      val thisRunTime = when($"timestamp" === experiment.timestamp, $"executionTime").otherwise(null)

      val data = sqlContext.read.json(tpcds.resultsLocation)
        .coalesce(1)
        .where(s"timestamp IN ($baseTimestamp, ${experiment.timestamp})")
        .withColumn("result", explode($"results"))
        .select("timestamp", "result.*")
        .groupBy("name")
        .agg(
          avg(baselineTime) as 'baselineTimeMs,
          avg(thisRunTime) as 'thisRunTimeMs,
          stddev(baselineTime) as 'stddev)
        .withColumn(
          "percentChange", ($"baselineTimeMs" - $"thisRunTimeMs") / $"baselineTimeMs" * 100)
        .filter('thisRunTimeMs.isNotNull)

      data.show(truncate = false)
    }
    sparkSession.stop()
  }
}
