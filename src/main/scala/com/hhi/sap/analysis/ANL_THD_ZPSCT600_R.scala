package com.hhi.sap.analysis

import com.hhi.sap.analysis.functions.ShipSimilarity
import com.hhi.sap.config.DateTimeUtil
import com.hhi.sap.table.bean.BEAN_ZPSCT_600_R
import com.hhi.sap.table.sql.SQL_MASTER
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class ANL_THD_ZPSCT600_R(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(tb_ZPDCT_600: DataFrame, tb_FACTORMASTER: DataFrame): DataFrame = genTable(tb_ZPDCT_600, tb_FACTORMASTER)

  private def genTable(tb_ZPDCT_600: DataFrame, tb_FACTORMASTER: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    var PGMID = "Spark2.3.0.cloudera2"
    var CNAM = "A504863"

    if (logger.isWarnEnabled) {
      PGMID = "[DEBUGMODE]Spark2.3.0.cloudera2"
      CNAM = "[DEBUGMODE]A504863"
    }

    val progressShip = tb_ZPDCT_600.where(SQL_MASTER.ZPSCT_600.SQL_COSTAT_N)
    val completeShip = tb_ZPDCT_600.where(SQL_MASTER.ZPSCT_600.SQL_COSTAT_Y)

    val factorMaster = tb_FACTORMASTER.collect()

    progressShip.rdd.cartesian(completeShip.rdd)
      .map(e=>
        BEAN_ZPSCT_600_R(
          e._1.getAs(TERM_MASTER.ZPSCT600.COMPANYID),
          e._1.getAs(TERM_MASTER.ZPSCT600.SAUPBU),
          e._1.getAs(TERM_MASTER.ZPSCT600.PSPID),
          e._2.getAs(TERM_MASTER.ZPSCT600.SHIP_KIND),
          e._2.getAs(TERM_MASTER.ZPSCT600.SHIP_TYPE_1),
          e._2.getAs(TERM_MASTER.ZPSCT600.DOCK),
          e._2.getAs(TERM_MASTER.ZPSCT600.BTYPE),
          e._2.getAs(TERM_MASTER.ZPSCT600.DUR_AND),
          e._2.getAs(TERM_MASTER.ZPSCT600.D1_ND),
          e._2.getAs(TERM_MASTER.ZPSCT600.D2_ND),
          e._2.getAs(TERM_MASTER.ZPSCT600.D3_ND),
          e._2.getAs(TERM_MASTER.ZPSCT600.DUR_QND),
          e._2.getAs(TERM_MASTER.ZPSCT600.CNTR),
          e._2.getAs(TERM_MASTER.ZPSCT600.DWT_SC),
          e._2.getAs(TERM_MASTER.ZPSCT600.WEIGT_PR),
          e._2.getAs(TERM_MASTER.ZPSCT600.WEIGT_BB),
          e._2.getAs(TERM_MASTER.ZPSCT600.WEIGT_LD),
          e._2.getAs(TERM_MASTER.ZPSCT600.WC),
          e._2.getAs(TERM_MASTER.ZPSCT600.PSPID),
          ShipSimilarity.getSimilarity(factorMaster, e._1,e._2).toString,
          PGMID,
          CNAM,
          DateTimeUtil.date,
          DateTimeUtil.time
        )
      ).toDF()
      .withColumn(TERM_MASTER.ZPSCT600_R.SERNO, row_number().over(Window.partitionBy(TERM_MASTER.ZPSCT600_R.PSPID).orderBy(TERM_MASTER.ZPSCT600_R.PSPID)))
      .withColumn(TERM_MASTER.ZPSCT600_R.RANKING, rank().over(Window.partitionBy(TERM_MASTER.ZPSCT600_R.PSPID).orderBy(TERM_MASTER.ZPSCT600_R.RANK_RATE)))
  }
}