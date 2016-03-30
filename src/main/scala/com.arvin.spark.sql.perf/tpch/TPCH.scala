package com.arvin.spark.sql.perf.tpch

import com.arvin.spark.sql.perf.{Query, Benchmark}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

class TPCH(ctx: SQLContext) extends Benchmark with Serializable {
  def queries(indices: Option[Seq[Int]]): Seq[Query] = {
    indices match {
      case Some(x) => TPCHQuery.withFilter(x, ctx)
      case None => TPCHQuery.allQueries(ctx)
    }
  }

  def explain(queries: Seq[Query], showPlan: Boolean = true): Unit = {
    val succeeded = mutable.ArrayBuffer.empty[String]
    queries.foreach { q =>
      println(s"Query: ${q.name}")
      try {
        val df = q.newDataFrame()
        if (showPlan) {
          df.explain(false)
        } else {
          df.queryExecution.executedPlan
        }
        succeeded += q.name
      } catch {
        case e: Exception =>
          println("Failed to plan: " + e)
      }
    }
    println(s"Planned ${succeeded.size} out of ${queries.size}")
    println(succeeded.map("\"" + _ + "\""))
  }
}
