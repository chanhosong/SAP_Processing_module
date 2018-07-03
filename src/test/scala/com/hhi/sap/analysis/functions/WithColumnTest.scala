package com.hhi.sap.analysis.functions

import com.hhi.sap.main.SparkSessionTestWrapper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec

class WithColumnTest extends FlatSpec with SparkSessionTestWrapper{
  case class TestBean(Hour: String, Category: String, TotalValue: String)

  "Table" should "make." in {
    import ss.sqlContext.implicits._

    val table = ss.sparkContext.parallelize(Seq(
      (0, "cat26", 30.9, "N"), (0, "cat13", 22.1, "Y"),  (0, "cat95", 19.6, "Y"),   (0, "cat105", 1.3, "N"),
      (1, "cat67", 28.5, "Y"), (1, "cat4", 26.8, "N"),   (1, "cat13", 12.6, "Y"),   (1, "cat23", 5.3, "N"),
      (2, "cat56", 39.6, "Y"), (2, "cat40", 29.7, "Y"),  (2, "cat187", 27.9, "N"),  (2, "cat68", 9.8, "Y"),
      (3, "cat8", 35.6, "Y"))).toDF("Hour", "Category", "TotalValue", "STAT")

    val progressShip = table.where("STAT = 'N'").as("N")
    val completeShip = table.where("STAT = 'Y'").as("Y")

    progressShip.rdd.cartesian(completeShip.rdd)
      .map(e=>
        TestBean(
          e._1.getAs("Hour"),
          e._2.getAs("Category"),
          e._2.getAs("TotalValue")
        )
      ).toDF()
      .withColumn("SERNO", row_number().over(Window.partitionBy("Category").orderBy("TotalValue")))
      .withColumn("RANKING", rank().over(Window.partitionBy("Category").orderBy("TotalValue")))
  }
}