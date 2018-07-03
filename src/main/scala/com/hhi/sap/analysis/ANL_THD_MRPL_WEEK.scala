package com.hhi.sap.analysis

import java.util.Calendar

import com.hhi.sap.analysis.functions.ProcessClassification
import com.hhi.sap.config.DateTimeUtil
import com.hhi.sap.table.bean.BEAN_THD_MRPL_WEEK
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

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
    val dtu = DateTimeUtil

    val date = dtu.date
    val time = dtu.time

    zpdct6023.rdd.map(e=>{
      BEAN_THD_MRPL_WEEK(
        e.getAs(TERM_MASTER.MRPL_WEEK.COMPANYID),
        e.getAs(TERM_MASTER.MRPL_WEEK.SAUPBU),
        e.getAs(TERM_MASTER.MRPL_WEEK.PSPID),
        e.getAs(TERM_MASTER.MRPL_WEEK.SERNO),
        ProcessClassification.getSTG_GUBUN(e.getAs(TERM_MASTER.ZPDCT6023.ZMIDACTNO), e.getAs(TERM_MASTER.ZPDCT6023.ZHDRMATNR)),
        ProcessClassification.getMAT_GUBUN(e.getAs(TERM_MASTER.ZPDCT6023.ZMIDACTNO), e.getAs(TERM_MASTER.EBAN.LGORT), e.getAs(TERM_MASTER.EBAN.PAINTGBN), e.getAs(TERM_MASTER.MARA.ZZMGROUP)),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCM5),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCM4),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCM3),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCM2),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCM1),
        e.getAs(TERM_MASTER.MRPL_WEEK.WC),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP1),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP2),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP3),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP4),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP5),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP6),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP7),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP8),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP9),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP10),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP11),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP12),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP13),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP14),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP15),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP16),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP17),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP18),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP19),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP20),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP21),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP22),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP23),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP24),
        e.getAs(TERM_MASTER.MRPL_WEEK.WCP25),
        PGMID,
        CNAM,
        date.format(Calendar.getInstance().getTime),
        time.format(Calendar.getInstance().getTime)
      )
    }).toDF()
  }
}