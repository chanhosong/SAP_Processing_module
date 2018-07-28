package com.hhi.sap.analysis.functions

import java.sql.Date

import com.hhi.sap.main.SparkSessionTestWrapper
import org.scalatest.FlatSpec
import org.apache.spark.sql.functions._

class DataFrameTest extends FlatSpec with SparkSessionTestWrapper{
  case class TestBean(Hour: String, Category: String, TotalValue: String)

  "simple code1" should "be tested." in {
    List("apple", "apple", "orange", "apple", "mango", "orange")
      .map(word => (word, 1)).groupBy(_._1)
      .map(word => (word._1, word._2.foldLeft(0)((sum,c) => sum+ c._2)))
      .foreach(println)
  }

  "simple code2" should "be tested." in {
    ss.sparkContext.parallelize(List(
      ("West",  "Apple",  2.0, 10),
      ("West",  "Apple",  3.0, 15),
      ("West",  "Orange", 5.0, 15),
      ("South", "Orange", 3.0, 9),
      ("South", "Orange", 6.0, 18),
      ("East",  "Milk",   5.0, 5)))
      .map{ case (store, prod, amt, units) => ((store, prod), (amt, amt, amt, units)) }
      .reduceByKey((x, y) => (x._1 + y._1, math.min(x._2, y._2), math.max(x._3, y._3), x._4 + y._4))
      .collect.foreach(println)
  }

  "simple code3" should "be tested." in {
    ss.sparkContext.parallelize(List(
      ("West",  "Apple1",  -9.0, 10),
      ("West",  "Orange", -8.0, 15),
      ("West",  "Apple1",  -4.0, 10),
      ("West",  "Apple1",  -5.0, 10),
      ("West",  "Apple",  2.0, 10),
      ("West",  "Apple",  3.0, 15),
      ("West",  "Orange", -1.0, 15),
      ("West",  "Orange", 5.0, 15),
      ("West",  "Orange", -2.0, 15),
      ("South", "Orange", 3.0, 9),
      ("South", "Orange", 1.0, 18),
      ("South", "Orange", 6.0, 18),
      ("East",  "Milk",   -3.0, 5),
      ("East",  "Milk",   5.0, 5),
      ("East",  "Milk1",   0.0, 5),
      ("East",  "Milk2",   -3.0, 5),
      ("East",  "Milk1",   5.0, 5),
      ("East",  "Milk2",   0.0, 5),
      ("South", "Orange", 1.0, 18),
      ("South", "Orange", 6.0, 18),
      ("East",  "Milk",   -3.0, 5)
    ))
      .map{ case (store, prod, amt, units) => ((store, prod), (amt, units)) }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map{ case ((store, prod), (amt, units)) => (store, prod, amt, units) }
      .groupBy(-3 until 10 contains _._3)
      .foreach(println)
  }

  "Dataframe column" should "be cumulative." in {
    import ss.sqlContext.implicits._

    val weatherDataDF = Seq(
      (100, Date.valueOf("2017-07-01"), 75, 59, 0.0),
      (100, Date.valueOf("2017-07-16"), 77, 59, 0.5),
      (100, Date.valueOf("2017-08-01"), 80, 63, 1.0),
      (100, Date.valueOf("2017-08-16"), 78, 62, 1.0),
      (100, Date.valueOf("2017-09-01"), 74, 59, 0.0),
      (100, Date.valueOf("2017-09-16"), 72, 57, 0.0),
      (100, Date.valueOf("2017-10-01"), 68, 54, 0.0),
      (100, Date.valueOf("2017-10-16"), 66, 54, 0.0),
      (100, Date.valueOf("2017-11-01"), 64, 50, 0.5),
      (100, Date.valueOf("2017-11-16"), 61, 48, 1.0),
      (100, Date.valueOf("2017-12-01"), 59, 46, 2.0),
      (100, Date.valueOf("2017-12-16"), 57, 45, 1.5),
      (115, Date.valueOf("2017-07-01"), 76, 57, 0.0),
      (115, Date.valueOf("2017-07-16"), 76, 56, 1.0),
      (115, Date.valueOf("2017-08-01"), 78, 57, 0.0),
      (115, Date.valueOf("2017-08-16"), 81, 57, 0.0),
      (115, Date.valueOf("2017-09-01"), 77, 54, 0.0),
      (115, Date.valueOf("2017-09-16"), 72, 50, 0.0),
      (115, Date.valueOf("2017-10-01"), 65, 45, 0.0),
      (115, Date.valueOf("2017-10-16"), 59, 40, 1.5),
      (115, Date.valueOf("2017-11-01"), 55, 37, 1.0),
      (115, Date.valueOf("2017-11-16"), 52, 35, 2.0),
      (115, Date.valueOf("2017-12-01"), 45, 30, 3.0),
      (115, Date.valueOf("2017-12-16"), 41, 28, 1.5)
    ).toDF("station", "start_date", "temp_high", "temp_low", "total_precip")

    ////
    val monthlyData = weatherDataDF.
      withColumn("year_mo", concat(
        year($"start_date"), lit("-"), lpad(month($"start_date"), 2, "0")
      )).groupBy("station").pivot("year_mo")

    val monthlyPrecipDF = monthlyData.agg(sum($"total_precip"))

    monthlyPrecipDF.show

    ////
    val yearMonths = monthlyPrecipDF.columns.filter(_ != "station")

    val cumulativePrecipDF = yearMonths.drop(1).
      foldLeft((monthlyPrecipDF, yearMonths.head))( (acc, c) =>
        ( acc._1.withColumn(c, col(acc._2) + col(c)), c )
      )._1

    cumulativePrecipDF.show

    ////
    val monthlyHighDF = monthlyData.agg(max($"temp_high").as("high"))

    monthlyHighDF.show

    val monthlyLowDF = monthlyData.agg(min($"temp_low").as("low"))

    monthlyLowDF.show
  }
}