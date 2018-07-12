package com.hhi.sap.analysis

import com.hhi.sap.analysis.functions.MRPLTableUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class ANL_THD_MRPL_WEEK(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(zpdct6123: DataFrame): DataFrame = genTable(zpdct6123)

  private def genTable(zpdct6123: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    var PGMID = "Spark2.3.0.cloudera2"
    var CNAM = "A504863"

    if (logger.isDebugEnabled) {
      PGMID = "[DEBUGMODE]Spark2.3.0.cloudera2"
      CNAM = "[DEBUGMODE]A504863"
    }

    val mrplRDD = MRPLTableUtils.getMRPLRDD(zpdct6123)
    val underRDD = MRPLTableUtils.getWeekTable(mrplRDD.filter(_.week <= -5), -5)
    val upperRDD = MRPLTableUtils.getWeekTable(mrplRDD.filter(_.week >= 20), 20)

    MRPLTableUtils
      .makeUnion(mrplRDD.filter(-4 until 19 contains _.week).toDF(), underRDD, upperRDD)
      .transform(MRPLTableUtils.addSERNO)
      .transform(MRPLTableUtils.pivotTable)
      .transform(MRPLTableUtils.mappingMRPLTable)
  }
}