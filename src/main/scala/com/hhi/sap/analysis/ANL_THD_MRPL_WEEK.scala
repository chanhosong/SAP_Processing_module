package com.hhi.sap.analysis

import com.hhi.sap.config.DateTimeUtil
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
    val dtu = new DateTimeUtil()

    val date = dtu.date
    val time = dtu.time

    zpdct6023//TODO
  }
}