package com.hhi.sap.analysis.functions

import com.hhi.sap.table.bean.{BEAN_THD_MRPL_WEEK, BEAN_THD_MRPL_WEEK_COUNT}
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.{Failure, Success, Try}

object MRPLTableGenerator {
  private val companyid = TERM_MASTER.ZPDCT6023.COMPANYID
  private val saupbu = TERM_MASTER.ZPDCT6023.SAUPBU
  private val pspid = TERM_MASTER.ZPDCT6023.PSPID
  private val serno = "serno".toUpperCase()
  private val stg_gubun = TERM_MASTER.ZPDCT6023.STG_GUBUN
  private val mat_gubun = TERM_MASTER.ZPDCT6023.MAT_GUBUN
  private val week = TERM_MASTER.ZPDCT6023.WEEK
  private val count = "count".toUpperCase()


  def getIntermediateRDD(df: DataFrame): RDD[BEAN_THD_MRPL_WEEK_COUNT] = {
    df.rdd.map(e => Tuple6(e.getAs(companyid).toString,
      e.getAs(saupbu).toString,
      e.getAs(pspid).toString,
      e.getAs(stg_gubun).toString,
      e.getAs(mat_gubun).toString,
      e.getAs(week).toString.toInt))
      .map { case (_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, _week) => ((_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, _week), 1) }
      .reduceByKey(_ + _)
      .map(e => BEAN_THD_MRPL_WEEK_COUNT(e._1._1, e._1._2, e._1._3, e._1._4, e._1._5, e._1._6, e._2))
  }

  def getWeekTable(sql: SQLContext, weekRDD: RDD[BEAN_THD_MRPL_WEEK_COUNT], weekNumber: Int): DataFrame = {
    import sql.sparkSession.implicits._

    weekRDD.map { BEAN_THD_MRPL_WEEK_TEMP => ((BEAN_THD_MRPL_WEEK_TEMP.companyid, BEAN_THD_MRPL_WEEK_TEMP.saupbu, BEAN_THD_MRPL_WEEK_TEMP.pspid, BEAN_THD_MRPL_WEEK_TEMP.stg_gubun, BEAN_THD_MRPL_WEEK_TEMP.mat_gubun), BEAN_THD_MRPL_WEEK_TEMP.count) }
      .reduceByKey(_ + _)
      .map { case ((_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun), _count) => (_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, weekNumber, _count) }
      .toDF(companyid, saupbu, pspid, stg_gubun, mat_gubun, week, count)
  }

  def getUnion(df: DataFrame, underRDD: DataFrame, upperRDD: DataFrame): DataFrame = df.union(underRDD).union(upperRDD)

  def addSERNO(df: DataFrame): DataFrame = df.withColumn(TERM_MASTER.MRPL_WEEK.SERNO, row_number().over(Window.partitionBy(TERM_MASTER.MRPL_WEEK.COMPANYID).partitionBy(TERM_MASTER.MRPL_WEEK.SAUPBU).partitionBy(TERM_MASTER.MRPL_WEEK.PSPID).orderBy(TERM_MASTER.MRPL_WEEK.PSPID)))

  def pivotTable(df: DataFrame, fill: Int): DataFrame = df.groupBy(companyid, saupbu, pspid, stg_gubun, mat_gubun, serno).pivot(week).sum(count).na.fill(fill)

  def mappingMRPLTable(sql: SQLContext, df: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    df.map(e => {
      BEAN_THD_MRPL_WEEK(
        e.getAs(companyid),
        e.getAs(saupbu),
        e.getAs(pspid),
        e.getAs(serno).toString,
        e.getAs(stg_gubun),
        e.getAs(mat_gubun),
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
        Try(e.getAs("20").toString) match { case Success(s) => s case Failure(s) => "0" }
      )
    }).toDF()
  }
}
