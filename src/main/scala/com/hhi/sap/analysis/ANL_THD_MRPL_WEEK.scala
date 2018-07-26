package com.hhi.sap.analysis

import com.hhi.sap.analysis.functions.MRPLTableUtils
import com.hhi.sap.analysis.functions.common.TransformUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class ANL_THD_MRPL_WEEK(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(zpdct6123: DataFrame): DataFrame = genTable(zpdct6123)

  private def genTable(zpdct6123: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    val mrplRDD = MRPLTableUtils.getMRPLRDD(zpdct6123)
    val underDF = MRPLTableUtils.getWeekTable(mrplRDD.filter(_.week <= -5), -5)
    val upperDF = MRPLTableUtils.getWeekTable(mrplRDD.filter(_.week >= 20), 20)

    TransformUtils
      .makeUnion(mrplRDD.filter(-4 until 19 contains _.week).toDF(), underDF, upperDF)
      .transform(TransformUtils.pivotTableByCount)
      .transform(TransformUtils.mappingTable)
      .transform(TransformUtils.addSERNO)
  }
}