package com.hhi.sap.analysis.functions

import com.hhi.sap.main.SparkSessionWrapper
import com.hhi.sap.table.bean.BEAN_THD_WEIGHT_WEEK_COUNT
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object WeightTableUtils extends SparkSessionWrapper{
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val COMPANYID = TERM_MASTER.ZPDCT6023.COMPANYID
  private val SAUPBU = TERM_MASTER.ZPDCT6023.SAUPBU
  private val PSPID = TERM_MASTER.ZPDCT6023.PSPID
  private val SERNO = "serno".toUpperCase()
  private val STG_GUBUN = TERM_MASTER.ZPDCT6023.STG_GUBUN
  private val MAT_GUBUN = TERM_MASTER.ZPDCT6023.MAT_GUBUN
  private val WEEK = TERM_MASTER.ZPDCT6023.WEEK
  private val MENGE = TERM_MASTER.ZPDCT6023.MENGE
  private val COUNT = "count".toUpperCase()

  private var PGMID = "Spark2.3.0.cloudera2"
  private var CNAM = "A504863"

  def getWeightRDD(zpdct6023: DataFrame, mara: DataFrame): RDD[BEAN_THD_WEIGHT_WEEK_COUNT] = {
    zpdct6023.rdd.map(e =>
      Tuple7(e.getAs(COMPANYID).toString,
        e.getAs(SAUPBU).toString,
        e.getAs(PSPID).toString,
        e.getAs(STG_GUBUN).toString,
        e.getAs(MAT_GUBUN).toString,
        e.getAs(WEEK).toString.toInt,
        e.getAs(MENGE).toString.toDouble))
      .map { case (_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, _week, _menge) => ((_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, _week), _menge) }
      .reduceByKey(_ + _)
      .map(e => BEAN_THD_WEIGHT_WEEK_COUNT(e._1._1, e._1._2, e._1._3, e._1._4, e._1._5, e._1._6, e._2))
  }

  def getWeightTable(weekRDD: RDD[BEAN_THD_WEIGHT_WEEK_COUNT], week: Int): DataFrame = {
    import ss.sqlContext.sparkSession.implicits._

    weekRDD.map { BEAN_THD_WEIGHT_WEEK_TEMP => ((BEAN_THD_WEIGHT_WEEK_TEMP.companyid, BEAN_THD_WEIGHT_WEEK_TEMP.saupbu, BEAN_THD_WEIGHT_WEEK_TEMP.pspid, BEAN_THD_WEIGHT_WEEK_TEMP.stg_gubun, BEAN_THD_WEIGHT_WEEK_TEMP.mat_gubun), BEAN_THD_WEIGHT_WEEK_TEMP.menge) }
      .reduceByKey(_ + _)
      .map { case ((_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun), _menge) => (_companyid, _saupbu, _pspid, _stg_gubun, _mat_gubun, week, _menge) }
      .toDF(COMPANYID, SAUPBU, PSPID, STG_GUBUN, MAT_GUBUN, WEEK, MENGE)
  }
}
