package com.hhi.sap.analysis

import com.hhi.sap.analysis.functions.WeightTableUtils
import com.hhi.sap.analysis.functions.common.TransformUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class ANL_THD_WEIGHT_WEEK(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(zpdct6023: DataFrame, mara: DataFrame): DataFrame = genTable(zpdct6023, mara)

  private def genTable(zpdct6023: DataFrame, mara: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    val weightRDD = WeightTableUtils.getWeightRDD(zpdct6023, mara)

    val underRDD = WeightTableUtils.getWeightTable(weightRDD.filter(_.week <= -5), -5)
    val upperRDD = WeightTableUtils.getWeightTable(weightRDD.filter(_.week >= 20), 20)

    TransformUtils
      .makeUnion(weightRDD.filter(-4 until 19 contains _.week).toDF(), underRDD, upperRDD)
      .transform(TransformUtils.addSERNO)
      .transform(TransformUtils.pivotTableByMenge)
      .transform(TransformUtils.mappingTable)
  }
}