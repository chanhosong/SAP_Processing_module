package com.hhi.sap.analysis

import com.hhi.sap.table.bean.{BEAN_THD_MRPL_WEEK, BEAN_THD_MRPL_WEEK_COUNT}
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class ANL_THD_MRPL_WEEK(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(zpdct6023: DataFrame): DataFrame = genTable(zpdct6023)

  private def genTable(zpdct6023: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    var PGMID = "Spark2.3.0.cloudera2"
    var CNAM = "A504863"

    if (logger.isWarnEnabled) {
      PGMID = "[DEBUGMODE]Spark2.3.0.cloudera2"
      CNAM = "[DEBUGMODE]A504863"
    }

    val intermediateRDD = zpdct6023.rdd
      .map(e=>Tuple6(e.getAs(TERM_MASTER.ZPDCT6023.COMPANYID).toString,
        e.getAs(TERM_MASTER.ZPDCT6023.SAUPBU).toString,
        e.getAs(TERM_MASTER.ZPDCT6023.PSPID).toString,
        e.getAs(TERM_MASTER.ZPDCT6023.STG_GUBUN).toString,
        e.getAs(TERM_MASTER.ZPDCT6023.MAT_GUBUN).toString,
        e.getAs(TERM_MASTER.ZPDCT6023.WEEK).toString.toInt))
      .map{ case (companyid, saupbu,  pspid, stg_gubun, mat_gubun, week) => ((companyid, saupbu, pspid, stg_gubun, mat_gubun, week), 1)}
      .reduceByKey(_+_)
      .map(e=> BEAN_THD_MRPL_WEEK_COUNT(e._1._1, e._1._2, e._1._3, e._1._4, e._1._5, e._1._6, e._2))

    val underRDD = week(intermediateRDD.filter(_.week <= -5), -5)
    val upperRDD = week(intermediateRDD.filter(_.week >= 20), 20)

    intermediateRDD.filter(-4 until 19 contains _.week).toDF().union(underRDD).union(upperRDD)
      .withColumn(TERM_MASTER.MRPL_WEEK.SERNO, row_number().over(Window.partitionBy(TERM_MASTER.MRPL_WEEK.COMPANYID).partitionBy(TERM_MASTER.MRPL_WEEK.SAUPBU).partitionBy(TERM_MASTER.MRPL_WEEK.PSPID).orderBy(TERM_MASTER.MRPL_WEEK.PSPID)))
      .groupBy("companyid","saupbu", "pspid", "stg_gubun", "mat_gubun", "serno").pivot("week").sum("count").na.fill(0)
      .map(e=>{
      BEAN_THD_MRPL_WEEK(
        e.getAs(TERM_MASTER.MRPL_WEEK.COMPANYID),
        e.getAs(TERM_MASTER.MRPL_WEEK.SAUPBU),
        e.getAs(TERM_MASTER.MRPL_WEEK.PSPID),
        e.getAs(TERM_MASTER.MRPL_WEEK.SERNO).toString,
        e.getAs(TERM_MASTER.MRPL_WEEK.STG_GUBUN),
        e.getAs(TERM_MASTER.MRPL_WEEK.MAT_GUBUN),
        Try(e.getAs("-5").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("-4").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("-3").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("-2").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("-1").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("0" ).toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("1" ).toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("2" ).toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("3" ).toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("4" ).toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("5" ).toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("6" ).toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("7" ).toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("8" ).toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("9" ).toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("10").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("11").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("12").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("13").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("14").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("15").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("16").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("17").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("18").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("19").toString) match {case Success(s) => s case Failure(s) => "0"},
        Try(e.getAs("20").toString) match {case Success(s) => s case Failure(s) => "0"}
      )
    }).toDF()
  }

  private def week(weekRDD: RDD[BEAN_THD_MRPL_WEEK_COUNT], week: Int): DataFrame = {
    import sql.sparkSession.implicits._

    weekRDD.map{ BEAN_THD_MRPL_WEEK_TEMP => ((BEAN_THD_MRPL_WEEK_TEMP.companyid, BEAN_THD_MRPL_WEEK_TEMP.saupbu, BEAN_THD_MRPL_WEEK_TEMP.pspid, BEAN_THD_MRPL_WEEK_TEMP.stg_gubun, BEAN_THD_MRPL_WEEK_TEMP.mat_gubun), BEAN_THD_MRPL_WEEK_TEMP.count)}
      .reduceByKey(_+_)
      .map{ case ((companyid, saupbu, pspid, stg_gubun, mat_gubun), count) => (companyid, saupbu, pspid, stg_gubun, mat_gubun, week, count)}
      .toDF("companyid", "saupbu", "pspid", "stg_gubun", "mat_gubun", "week", "count")
  }
}