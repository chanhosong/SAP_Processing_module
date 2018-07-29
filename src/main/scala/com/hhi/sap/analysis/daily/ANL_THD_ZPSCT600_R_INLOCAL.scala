package com.hhi.sap.analysis.daily

import com.hhi.sap.analysis.functions.ShipSimilarity_INLOCAL
import com.hhi.sap.analysis.functions.common.DateTimeUtil
import com.hhi.sap.table.bean.BEAN_ZPSCT_600_R
import com.hhi.sap.table.factor.FactorMasterTableFromLocal
import com.hhi.sap.table.sql.SQL_MASTER
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class ANL_THD_ZPSCT600_R_INLOCAL(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def runFromFile(tb_THD_ZPDCV6021: DataFrame): DataFrame = genTableFromFile(tb_THD_ZPDCV6021)

  private def genTableFromFile(tb_THD_ZPDCV6021: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    var PGMID = "Spark2.3.0.cloudera2"
    var CNAM = "A504863"

    if (logger.isDebugEnabled()) {
      PGMID = "[DEBUGMODE]Spark2.3.0.cloudera2"
      CNAM = "[DEBUGMODE]A504863"
    }

    val factor = FactorMasterTableFromLocal.getTable.collect()

    val progressShip = tb_THD_ZPDCV6021.where(SQL_MASTER.ZPSCT_600.SQL_COSTAT_N).as("N")
    val completeShip = tb_THD_ZPDCV6021.where(SQL_MASTER.ZPSCT_600.SQL_COSTAT_Y).as("Y")

    progressShip.rdd.cartesian(completeShip.rdd)
      .map(e=>
        BEAN_ZPSCT_600_R(
          e._1.getAs(TERM_MASTER.ZPSCT600.COMPANYID.toUpperCase()),
          e._1.getAs(TERM_MASTER.ZPSCT600.SAUPBU.toUpperCase()),
          e._1.getAs(TERM_MASTER.ZPSCT600.PSPID.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.SHIP_KIND.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.SHIP_TYPE_1.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.DOCK.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.BTYPE.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.DUR_AND.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.D1_ND.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.D2_ND.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.D3_ND.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.DUR_QND.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.CNTR.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.DWT_SC.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.WEIGT_PR.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.WEIGT_BB.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.WEIGT_LD.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.WC.toUpperCase()),
          e._2.getAs(TERM_MASTER.ZPSCT600.PSPID.toUpperCase()),
          ShipSimilarity_INLOCAL.getSimilarityFromFile(factor, e._1,e._2).toString,
          PGMID,
          CNAM,
          DateTimeUtil.date,
          DateTimeUtil.time
        )
      ).toDF()
    /**
      * In localhost, following code does not running on localhost.
      * So, it should be running on the [[factor//com.hhi.sap.table.THD_ZPSCT600_RTest]].
      * */
      .withColumn(TERM_MASTER.ZPSCT600_R.SERNO, row_number().over(Window.partitionBy(TERM_MASTER.ZPSCT600_R.PSPID).orderBy(TERM_MASTER.ZPSCT600_R.PSPID)))
      .withColumn(TERM_MASTER.ZPSCT600_R.RANKING, rank().over(Window.partitionBy(TERM_MASTER.ZPSCT600_R.PSPID).orderBy(TERM_MASTER.ZPSCT600_R.RANK_RATE)))
  }
}