package com.arvin.spark.sql.perf

import com.arvin.spark.sql.perf.tpch.{TPCH, Tables}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

//import scopt.OptionParser

case class RunConfig(
  queries: Option[String] = None,
  mode: String = "EXEC",
  iterations: Int = 3,
  breakdownEnabled: Boolean = false,
  dataPath: String = null,
  format: String = "orc")

/**
  * Run TPC-H Benchmark
  */
object Run {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(getClass.getName)
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new HiveContext(sc)

    val tables = new Tables(sqlContext)
    tables.createTemporaryTables(config.dataPath, config.format)

    val tpch = new TPCH(sqlContext)
    val queries = tpch.queries(config.queries.map(_.split(',').map(_.toInt)))

    if (config.mode.equals("EXP")) {
      tpch.explain(queries, true)
    } else {
      val status = tpch.runExperiment(queries, config.breakdownEnabled, config.iterations)
      status.waitForFinish(500)

      import sqlContext.implicits._

      sqlContext.setConf("spark.sql.shuffle.partitions", "1")
      sqlContext.setConf("spark.sql.vectorize.enabled", "false")
      sqlContext.setConf("spark.sql.vectorize.agg.enabled", "false")
      status.getCurrentRuns()
        .withColumn("result", explode($"results"))
        .select("result.*")
        .groupBy("name")
        .agg(
          min($"executionTime") as 'minTimeMs,
          max($"executionTime") as 'maxTimeMs,
          avg($"executionTime") as 'avgTimeMs,
          stddev($"executionTime") as 'stdDev)
        .orderBy("name")
        .show(truncate = false)
      println(s"""Results: sqlContext.read.json("${status.resultPath}")""")
    }
  }


}
