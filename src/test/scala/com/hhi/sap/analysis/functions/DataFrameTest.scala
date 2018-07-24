package com.hhi.sap.analysis.functions

import com.hhi.sap.main.SparkSessionTestWrapper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec

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
}