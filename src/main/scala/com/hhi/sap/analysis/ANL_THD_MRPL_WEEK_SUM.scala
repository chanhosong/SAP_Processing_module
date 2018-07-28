package com.hhi.sap.analysis

import com.hhi.sap.analysis.functions.MRPLTableUtils
import com.hhi.sap.analysis.functions.common.TransformUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class ANL_THD_MRPL_WEEK_SUM(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(zpdct6123: DataFrame): DataFrame = genTable(zpdct6123)

  private def genTable(zpdct6123: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    val mrplRDD = MRPLTableUtils.getMRPLRDDByWeek(zpdct6123)
    val underDF = MRPLTableUtils.getWeekTable(mrplRDD.filter(_.week <= -5), -5)
    val upperDF = MRPLTableUtils.getWeekTable(mrplRDD.filter(_.week >= 25), 25)

    TransformUtils
      .makeUnion(mrplRDD.filter(-4 until 24 contains _.week).toDF(), underDF, upperDF)
      .transform(TransformUtils.pivotWeekTableByCount)
      .transform(TransformUtils.mappingTableByWeek)
      .transform(TransformUtils.addSERNOByWeek)
      .transform(TransformUtils.cumulativeTable)
  }
}