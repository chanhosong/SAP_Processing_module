package com.hhi.sap.analysis.functions

import com.hhi.sap.main.SparkSessionWrapper
import com.hhi.sap.table.bean.{BEAN_THD_AMT_MONTH_COUNT, BEAN_THD_AMT_WEEK_COUNT}
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object AMTTableUtils extends SparkSessionWrapper{
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val COMPANYID = TERM_MASTER.ZPDCT6123.COMPANYID
  private val SAUPBU = TERM_MASTER.ZPDCT6123.SAUPBU
  private val PSPID = TERM_MASTER.ZPDCT6123.PSPID
  private val SERNO = "serno".toUpperCase()
  private val STG_GUBUN = TERM_MASTER.ZPDCT6123.STG_GUBUN
  private val MAT_GUBUN = TERM_MASTER.ZPDCT6123.MAT_GUBUN
  private val MENGE = TERM_MASTER.ZPDCT6123.MENGE
  private val MEINS = TERM_MASTER.ZPDCT6123.MEINS
  private val WERKS = TERM_MASTER.ZPDCT6123.WERKS
  private val ZZMGROUP = TERM_MASTER.MARA.ZZMGROUP
  private val SBDKZ = TERM_MASTER.MARC.SBDKZ
  private val VERPR = TERM_MASTER.QBEW.VERPR
  private val PEINH = TERM_MASTER.QBEW.PEINH
  private val WEEK = TERM_MASTER.ZPDCT6123.WEEK
  private val MONTH = TERM_MASTER.ZPDCT6123.MONTH
  private val AMOUNT = "amount".toUpperCase()

  private var PGMID = "Spark2.3.0.cloudera2"
  private var CNAM = "A504863"

  def getAMTRDDByWeek(zpdct6123: DataFrame): RDD[BEAN_THD_AMT_WEEK_COUNT] = {

    zpdct6123.rdd.map(e => Tuple13(
      e.getAs(COMPANYID).toString,
      e.getAs(SAUPBU).toString,
      e.getAs(PSPID).toString,
      e.getAs(STG_GUBUN).toString,
      e.getAs(MAT_GUBUN).toString,
      e.getAs(MENGE).toString.toDouble,
      e.getAs(MEINS).toString,
      e.getAs(WERKS).toString,
      if(Option(e.getAs(ZZMGROUP)).isDefined) e.getAs(ZZMGROUP).toString else "NULL",
      if(Option(e.getAs(SBDKZ)).isDefined) e.getAs(SBDKZ).toString else "NULL",
      e.getAs(VERPR).toString.toDouble,
      e.getAs(PEINH).toString.toDouble,
      e.getAs(WEEK).toString.toInt))
      .map { case (_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, _menge, _meins, _werks, _zzmgroup, _sbdkz, _verpr, _peinh, _week) => ((_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, _week), if(_meins.matches("MM") && _sbdkz.matches("2") && _zzmgroup.matches("CP|CQ")) _verpr * _menge * 100.0 else _verpr / _peinh * _menge * 100.0)}
      .reduceByKey(_+_)
      .map(e => BEAN_THD_AMT_WEEK_COUNT(e._1._1, e._1._2, e._1._3, e._1._4, e._1._5, e._1._6, e._2))
  }

  def getAMTRDDByMonth(zpdct6123: DataFrame): RDD[BEAN_THD_AMT_MONTH_COUNT] = {

    zpdct6123.rdd.map(e => Tuple13(
      e.getAs(COMPANYID).toString,
      e.getAs(SAUPBU).toString,
      e.getAs(PSPID).toString,
      e.getAs(STG_GUBUN).toString,
      e.getAs(MAT_GUBUN).toString,
      e.getAs(MENGE).toString.toDouble,
      e.getAs(MEINS).toString,
      e.getAs(WERKS).toString,
      if(Option(e.getAs(ZZMGROUP)).isDefined) e.getAs(ZZMGROUP).toString else "NULL",
      if(Option(e.getAs(SBDKZ)).isDefined) e.getAs(SBDKZ).toString else "NULL",
      e.getAs(VERPR).toString.toDouble,
      e.getAs(PEINH).toString.toDouble,
      e.getAs(MONTH).toString.toInt))
      .map { case (_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, _menge, _meins, _werks, _zzmgroup, _sbdkz, _verpr, _peinh, _month) => ((_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, _month), if(_meins.matches("MM") && _sbdkz.matches("2") && _zzmgroup.matches("CP|CQ")) _verpr * _menge * 100.0 else _verpr / _peinh * _menge * 100.0)}
      .reduceByKey(_+_)
      .map(e => BEAN_THD_AMT_MONTH_COUNT(e._1._1, e._1._2, e._1._3, e._1._4, e._1._5, e._1._6, e._2))
  }

  def getWeekTable(weekRDD: RDD[BEAN_THD_AMT_WEEK_COUNT], weekNumber: Int): DataFrame = {
    import ss.sqlContext.sparkSession.implicits._

    weekRDD.map { BEAN_THD_AMT_WEEK_TEMP => ((BEAN_THD_AMT_WEEK_TEMP.companyid, BEAN_THD_AMT_WEEK_TEMP.saupbu, BEAN_THD_AMT_WEEK_TEMP.pspid, BEAN_THD_AMT_WEEK_TEMP.stg_gubun, BEAN_THD_AMT_WEEK_TEMP.mat_gubun), BEAN_THD_AMT_WEEK_TEMP.amount) }
      .reduceByKey(_+_)
      .map { case ((_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun), _amount) => (_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, weekNumber, _amount) }
      .toDF(COMPANYID, SAUPBU, PSPID, STG_GUBUN, MAT_GUBUN, WEEK, AMOUNT)
  }

  def getMonthTable(weekRDD: RDD[BEAN_THD_AMT_MONTH_COUNT], monthNumber: Int): DataFrame = {
    import ss.sqlContext.sparkSession.implicits._

    weekRDD.map { BEAN_THD_AMT_MONTH_TEMP => ((BEAN_THD_AMT_MONTH_TEMP.companyid, BEAN_THD_AMT_MONTH_TEMP.saupbu, BEAN_THD_AMT_MONTH_TEMP.pspid, BEAN_THD_AMT_MONTH_TEMP.stg_gubun, BEAN_THD_AMT_MONTH_TEMP.mat_gubun), BEAN_THD_AMT_MONTH_TEMP.amount) }
      .reduceByKey(_+_)
      .map { case ((_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun), _amount) => (_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, monthNumber, _amount) }
      .toDF(COMPANYID, SAUPBU, PSPID, STG_GUBUN, MAT_GUBUN, MONTH, AMOUNT)
  }
}
