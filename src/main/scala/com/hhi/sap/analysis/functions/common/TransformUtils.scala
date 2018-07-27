package com.hhi.sap.analysis.functions.common

import com.hhi.sap.analysis.functions.WeightTableUtils._
import DateTimeUtil
import com.hhi.sap.table.bean.{BEAN_THD_MONTH, BEAN_THD_WEEK}
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object TransformUtils {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val COMPANYID = TERM_MASTER.ZPDCT6023.COMPANYID
  private val SAUPBU = TERM_MASTER.ZPDCT6023.SAUPBU
  private val PSPID = TERM_MASTER.ZPDCT6023.PSPID
  private val SERNO = "serno".toUpperCase()
  private val STG_GUBUN = TERM_MASTER.ZPDCT6023.STG_GUBUN
  private val MAT_GUBUN = TERM_MASTER.ZPDCT6023.MAT_GUBUN
  private val MONTH = TERM_MASTER.ZPDCT6023.MONTH
  private val WEEK = TERM_MASTER.ZPDCT6023.WEEK
  private val BRGEW = TERM_MASTER.ZPDCT6023.BRGEW
  private val MENGE = TERM_MASTER.ZPDCT6023.MENGE
  private val COUNT = "count".toUpperCase()
  private val AMOUNT = "amount".toUpperCase()

  private var PGMID = "Spark2.3.0.cloudera2"
  private var CNAM = "A504863"

  def makeUnion(df: DataFrame, underRDD: DataFrame, upperRDD: DataFrame): DataFrame = df.union(underRDD).union(upperRDD)

  def addSERNOByWeek(df: DataFrame): DataFrame = df.withColumn(TERM_MASTER.WEEK.SERNO, row_number().over(Window.partitionBy(TERM_MASTER.WEEK.COMPANYID).partitionBy(TERM_MASTER.WEEK.SAUPBU).partitionBy(TERM_MASTER.WEEK.PSPID).orderBy(TERM_MASTER.WEEK.PSPID)))

  def addSERNOByMonth(df: DataFrame): DataFrame = df.withColumn(TERM_MASTER.MONTH.SERNO, row_number().over(Window.partitionBy(TERM_MASTER.MONTH.COMPANYID).partitionBy(TERM_MASTER.MONTH.SAUPBU).partitionBy(TERM_MASTER.MONTH.PSPID).orderBy(TERM_MASTER.MONTH.PSPID)))

  def pivotWeekTableByBrgew(df: DataFrame): DataFrame = df.groupBy(COMPANYID, SAUPBU, PSPID, STG_GUBUN, MAT_GUBUN).pivot(WEEK).sum(BRGEW).na.fill(0)

  def pivotWeekTableByCount(df: DataFrame): DataFrame = df.groupBy(COMPANYID, SAUPBU, PSPID, STG_GUBUN, MAT_GUBUN).pivot(WEEK).sum(COUNT).na.fill(0)

  def pivotWeekTableByAmount(df: DataFrame): DataFrame = df.groupBy(COMPANYID, SAUPBU, PSPID, STG_GUBUN, MAT_GUBUN).pivot(WEEK).sum(AMOUNT).na.fill(0)

  def pivotMonthTableByBrgew(df: DataFrame): DataFrame = df.groupBy(COMPANYID, SAUPBU, PSPID, STG_GUBUN, MAT_GUBUN).pivot(MONTH).sum(BRGEW).na.fill(0)

  def pivotMonthTableByCount(df: DataFrame): DataFrame = df.groupBy(COMPANYID, SAUPBU, PSPID, STG_GUBUN, MAT_GUBUN).pivot(MONTH).sum(COUNT).na.fill(0)

  def pivotMonthTableByAmount(df: DataFrame): DataFrame = df.groupBy(COMPANYID, SAUPBU, PSPID, STG_GUBUN, MAT_GUBUN).pivot(MONTH).sum(AMOUNT).na.fill(0)

  def mappingTableByWeek(df: DataFrame): DataFrame = {
    import ss.sqlContext.sparkSession.implicits._

    if (logger.isDebugEnabled) {
      PGMID = "[DEBUGMODE]Spark2.3.0.cloudera2"
      CNAM = "[DEBUGMODE]A504863"
    }

    df.map(e => {
      BEAN_THD_WEEK(
        e.getAs(COMPANYID),
        e.getAs(SAUPBU),
        e.getAs(PSPID),
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
        Try(e.getAs("21").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("22").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("23").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("24").toString) match { case Success(s) => s case Failure(s) => "0" },
        Try(e.getAs("25").toString) match { case Success(s) => s case Failure(s) => "0" },
        PGMID,
        CNAM,
        DateTimeUtil.date,
        DateTimeUtil.time
      )
    }).toDF()
  }

  def mappingTableByMonth(df: DataFrame): DataFrame = {
    import ss.sqlContext.sparkSession.implicits._

    if (logger.isDebugEnabled) {
      PGMID = "[DEBUGMODE]Spark2.3.0.cloudera2"
      CNAM = "[DEBUGMODE]A504863"
    }

    df.map(e => {
      BEAN_THD_MONTH(
        e.getAs(COMPANYID),
        e.getAs(SAUPBU),
        e.getAs(PSPID),
        e.getAs(STG_GUBUN),
        e.getAs(MAT_GUBUN),
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
        PGMID,
        CNAM,
        DateTimeUtil.date,
        DateTimeUtil.time
      )
    }).toDF()
  }
}
