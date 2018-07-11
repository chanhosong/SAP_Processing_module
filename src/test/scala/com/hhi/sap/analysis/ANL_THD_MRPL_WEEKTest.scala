package com.hhi.sap.analysis

import com.hhi.sap.main.SparkSessionTestWrapper
import com.hhi.sap.table.term.TERM_MASTER
import com.hhi.sap.utils.SparkFileReader
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class ANL_THD_MRPL_WEEKTest extends FlatSpec with SparkSessionTestWrapper{
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val INPUTPATH = "src/test/resources"
  private val TABLE4 = "/output/table4"
  private val FILENPATH_ZPSCT600 = "/ZPDCV6021/*"
  private val FILENPATH_ZPDCT6023 = "/ZPDCT6023/*"
  private val FILEPATH_EBAN = "/EBAN/*"
  private val FILEPATH_MARA = "/MARA/*"

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
    import ss.sqlContext.sparkSession.implicits._

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
//      .toDF("store", "prod", "amt", "units")
//      .where("amt < -1 ")
//      .show()
  }

  "ZPDCT6023" should "be counted." in new SparkFileReader {
    println(getFolder(INPUTPATH+FILENPATH_ZPDCT6023).count())
  }

  "ZPDCT6023 " should "make dataframe." in new SparkFileReader {
    //"Please be generated a table ZPDCT6023 on class ANL_THD_ZPSCT600_RTest "
    val tb_ZPDCT6023 = ss.read.option("header", "true").csv(INPUTPATH + TABLE4)

    new ANL_THD_MRPL_WEEKLY(ss.sqlContext).run(tb_ZPDCT6023
      .select(TERM_MASTER.ZPDCT6023.COMPANYID, TERM_MASTER.ZPDCT6023.SAUPBU, TERM_MASTER.ZPDCT6023.ZTRKNO, TERM_MASTER.ZPDCT6023.ZREVNO, TERM_MASTER.ZPDCT6023.PSPID, TERM_MASTER.ZPDCT6023.STG_GUBUN, TERM_MASTER.ZPDCT6023.MAT_GUBUN, TERM_MASTER.ZPDCT6023.WEEK))
      .show()
  }
}