package com.hhi.sap.data.dataframe

import com.hhi.sap.main.SparkSessionTestWrapper
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class UnionDataframe extends FlatSpec with SparkSessionTestWrapper{
  private val logger = LoggerFactory.getLogger(this.getClass)

  "Dataframe" should "be make." in {
    import ss.sqlContext.implicits._
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.row_number

    try {
      ss.sparkContext.parallelize(Seq(
        (0, "cat26", 30.9), (0, "cat13", 22.1), (0, "cat95", 19.6), (0, "cat105", 1.3),
        (1, "cat67", 28.5), (1, "cat4", 26.8), (1, "cat13", 12.6), (1, "cat23", 5.3),
        (2, "cat56", 39.6), (2, "cat40", 29.7), (2, "cat187", 27.9), (2, "cat68", 9.8),
        (3, "cat8", 35.6))).toDF("Hour", "Category", "TotalValue")
        .withColumn("rn", row_number.over(Window.partitionBy($"hour").orderBy($"TotalValue".desc)))
        .where($"rn" === 1)
        .drop("rn")
        .show()
    } catch {
      case  e: IllegalArgumentException => {
        logger.error(e.getMessage)
        logger.error(s"You running on the jdk 10, But You have to change your the jdk version. Recommended version 8.")
      }
    }
  }

  "Dataframe" should "be make to parquet." in {
    import ss.sqlContext.implicits._
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.row_number

    val OUTPUTPATH = "src/test/resources/parquet/test"

    try {
      ss.sparkContext.parallelize(Seq(
        (0, "cat26", 30.9), (0, "cat13", 22.1), (0, "cat95", 19.6), (0, "cat105", 1.3),
        (1, "cat67", 28.5), (1, "cat4", 26.8), (1, "cat13", 12.6), (1, "cat23", 5.3),
        (2, "cat56", 39.6), (2, "cat40", 29.7), (2, "cat187", 27.9), (2, "cat68", 9.8),
        (3, "cat8", 35.6))).toDF("Hour", "Category", "TotalValue")
        .withColumn("rn", row_number.over(Window.partitionBy($"hour").orderBy($"TotalValue".desc)))
        .where($"rn" === 1).write.parquet(OUTPUTPATH)
    } catch {
      case  e: IllegalArgumentException => {
        logger.error(e.getMessage)
        logger.error(s"You running on the jdk 10, But You have to change your the jdk version. Recommended version 8.")
      }
    }
  }

  "Parquet" should "be read." in {
    val READPATH = "src/test/resources/parquet/test"

    try {
      ss.read.parquet(READPATH).show()
    } catch {
      case  e: IllegalArgumentException => {
        logger.error(e.getMessage)
      }
    }
  }
}