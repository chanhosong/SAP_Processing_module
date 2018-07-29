package com.hhi.sap.analysis.monthly

import com.hhi.sap.analysis.functions.MRPLTableUtils
import com.hhi.sap.analysis.functions.common.TransformUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class ANL_THD_MRPL_MONTH(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(zpdct6123: DataFrame): DataFrame = genTable(zpdct6123)

  private def genTable(zpdct6123: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    val mrplRDD = MRPLTableUtils.getMRPLRDDByMonth(zpdct6123)
    val underDF = MRPLTableUtils.getMonthTable(mrplRDD.filter(_.month <= -2), -2)
    val upperDF = MRPLTableUtils.getMonthTable(mrplRDD.filter(_.month >= 10), 10)

    TransformUtils
      .makeUnion(mrplRDD.filter(-1 until 9 contains _.month).toDF(), underDF, upperDF)
      .transform(TransformUtils.pivotMonthTableByCount)
      .transform(TransformUtils.mappingTableByMonth)
      .transform(TransformUtils.addSERNOByMonth)
  }
}