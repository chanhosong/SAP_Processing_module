package com.hhi.sap.analysis.functions

import com.hhi.sap.config.DateTimeUtil
import com.hhi.sap.main.SparkSessionWrapper
import com.hhi.sap.table.bean.{BEAN_THD_MRPL_WEEK, BEAN_THD_MRPL_WEEK_COUNT}
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object MRPLTableUtils extends SparkSessionWrapper{
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val COMPANYID = TERM_MASTER.ZPDCT6123.COMPANYID
  private val SAUPBU = TERM_MASTER.ZPDCT6123.SAUPBU
  private val PSPID = TERM_MASTER.ZPDCT6123.PSPID
  private val SERNO = "serno".toUpperCase()
  private val STG_GUBUN = TERM_MASTER.ZPDCT6123.STG_GUBUN
  private val MAT_GUBUN = TERM_MASTER.ZPDCT6123.MAT_GUBUN
  private val WEEK = TERM_MASTER.ZPDCT6123.WEEK
  private val COUNT = "count".toUpperCase()

  private var PGMID = "Spark2.3.0.cloudera2"
  private var CNAM = "A504863"

  def getMRPLRDD(df: DataFrame): RDD[BEAN_THD_MRPL_WEEK_COUNT] = {
    df.rdd.map(e => Tuple6(e.getAs(COMPANYID).toString,
      e.getAs(SAUPBU).toString,
      e.getAs(PSPID).toString,
      e.getAs(STG_GUBUN).toString,
      e.getAs(MAT_GUBUN).toString,
      e.getAs(WEEK).toString.toInt))
      .map { case (_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, _week) => ((_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, _week), 1) }
      .reduceByKey(_ + _)
      .map(e => BEAN_THD_MRPL_WEEK_COUNT(e._1._1, e._1._2, e._1._3, e._1._4, e._1._5, e._1._6, e._2))
  }

  def getWeekTable(weekRDD: RDD[BEAN_THD_MRPL_WEEK_COUNT], weekNumber: Int): DataFrame = {
    import ss.sqlContext.sparkSession.implicits._

    weekRDD.map { BEAN_THD_MRPL_WEEK_TEMP => ((BEAN_THD_MRPL_WEEK_TEMP.companyid, BEAN_THD_MRPL_WEEK_TEMP.saupbu, BEAN_THD_MRPL_WEEK_TEMP.pspid, BEAN_THD_MRPL_WEEK_TEMP.stg_gubun, BEAN_THD_MRPL_WEEK_TEMP.mat_gubun), BEAN_THD_MRPL_WEEK_TEMP.count) }
      .reduceByKey(_ + _)
      .map { case ((_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun), _count) => (_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, weekNumber, _count) }
      .toDF(COMPANYID, SAUPBU, PSPID, STG_GUBUN, MAT_GUBUN, WEEK, COUNT)
  }

  def makeUnion(df: DataFrame, underRDD: DataFrame, upperRDD: DataFrame): DataFrame = df.union(underRDD).union(upperRDD)

  def addSERNO(df: DataFrame): DataFrame = df.withColumn(TERM_MASTER.MRPL_WEEK.SERNO, row_number().over(Window.partitionBy(TERM_MASTER.MRPL_WEEK.COMPANYID).partitionBy(TERM_MASTER.MRPL_WEEK.SAUPBU).partitionBy(TERM_MASTER.MRPL_WEEK.PSPID).orderBy(TERM_MASTER.MRPL_WEEK.PSPID)))

  def pivotTable(df: DataFrame): DataFrame = df.groupBy(COMPANYID, SAUPBU, PSPID, STG_GUBUN, MAT_GUBUN, SERNO).pivot(WEEK).sum(COUNT).na.fill(0)

  def mappingMRPLTable(df: DataFrame): DataFrame = {
    import ss.sqlContext.sparkSession.implicits._

    if (logger.isDebugEnabled) {
      PGMID = "[DEBUGMODE]Spark2.3.0.cloudera2"
      CNAM = "[DEBUGMODE]A504863"
    }

    df.map(e => {
      BEAN_THD_MRPL_WEEK(
        e.getAs(COMPANYID),
        e.getAs(SAUPBU),
        e.getAs(PSPID),
        e.getAs(SERNO).toString,
        e.getAs(STG_GUBUN),
        e.getAs(MAT_GUBUN),
        Try(e.getAs("-5").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("-4").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("-3").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("-2").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("-1").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("0").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("1").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("2").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("3").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("4").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("5").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("6").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("7").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("8").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("9").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("10").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("11").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("12").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("13").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("14").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("15").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("16").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("17").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("18").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("19").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("20").toString) match { case Success(s) => s case Failure(s) => "0" },
        PGMID,
        CNAM,
        DateTimeUtil.date,
        DateTimeUtil.time
      )
    }).toDF()
  }
}
