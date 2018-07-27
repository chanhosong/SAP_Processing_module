package com.hhi.sap.analysis

import com.hhi.sap.analysis.functions.WeightTableUtils
import com.hhi.sap.analysis.functions.common.TransformUtils
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class ANL_THD_WEIGHT_MONTH(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(zpdct6023: DataFrame, mara: DataFrame): DataFrame = genTable(zpdct6023, mara)

  private def genTable(zpdct6023: DataFrame, mara: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    val joinedRDD = zpdct6023.as(TERM_MASTER.ZPDCT6023.TABLENAME)
      .join(mara.as(TERM_MASTER.MARA.TABLENAME), zpdct6023(TERM_MASTER.ZPDCT6023.COMPANYID) <=> mara(TERM_MASTER.MARA.COMPANYID) && zpdct6023(TERM_MASTER.ZPDCT6023.SAUPBU) <=> mara(TERM_MASTER.MARA.SAUPBU) && zpdct6023(TERM_MASTER.ZPDCT6023.IDNRK) <=> mara(TERM_MASTER.MARA.MATNR), "left")
      .select(
        s"${TERM_MASTER.ZPDCT6023.TABLENAME}.${TERM_MASTER.ZPDCT6023.COMPANYID}"
        ,s"${TERM_MASTER.ZPDCT6023.TABLENAME}.${TERM_MASTER.ZPDCT6023.SAUPBU}"
        , s"${TERM_MASTER.ZPDCT6023.TABLENAME}.${TERM_MASTER.ZPDCT6023.PSPID}"
        , s"${TERM_MASTER.ZPDCT6023.TABLENAME}.${TERM_MASTER.ZPDCT6023.STG_GUBUN}"
        , s"${TERM_MASTER.ZPDCT6023.TABLENAME}.${TERM_MASTER.ZPDCT6023.MAT_GUBUN}"
        , s"${TERM_MASTER.ZPDCT6023.TABLENAME}.${TERM_MASTER.ZPDCT6023.MATNR}"
        , s"${TERM_MASTER.ZPDCT6023.TABLENAME}.${TERM_MASTER.ZPDCT6023.IDNRK}"
        , s"${TERM_MASTER.ZPDCT6023.TABLENAME}.${TERM_MASTER.ZPDCT6023.MENGE}"
        , s"${TERM_MASTER.ZPDCT6023.TABLENAME}.${TERM_MASTER.ZPDCT6023.BRGEW}"
        , s"${TERM_MASTER.MARA.TABLENAME}.${TERM_MASTER.MARA.BRGEW}"
        , s"${TERM_MASTER.ZPDCT6023.TABLENAME}.${TERM_MASTER.ZPDCT6023.MONTH}"
      ).toDF(
      TERM_MASTER.ZPDCT6023.COMPANYID,
      TERM_MASTER.ZPDCT6023.SAUPBU,
      TERM_MASTER.ZPDCT6023.PSPID,
      TERM_MASTER.ZPDCT6023.STG_GUBUN,
      TERM_MASTER.ZPDCT6023.MAT_GUBUN,
      TERM_MASTER.ZPDCT6023.MATNR,
      TERM_MASTER.ZPDCT6023.IDNRK,
      TERM_MASTER.ZPDCT6023.MENGE,
      TERM_MASTER.ZPDCT6023.BRGEW,
      TERM_MASTER.MARA.BRGEW+"_A",
      TERM_MASTER.ZPDCT6023.MONTH
    )

    val weightRDD = WeightTableUtils.getWeightRDDByMonth(joinedRDD, mara)
    val underDF = WeightTableUtils.getWeightMonthTable(weightRDD.filter(_.month <= -2), -2)
    val upperDF = WeightTableUtils.getWeightMonthTable(weightRDD.filter(_.month >= 10), 10)

    TransformUtils
      .makeUnion(weightRDD.filter(-1 until 9 contains _.month).toDF(), underDF, upperDF)
      .transform(TransformUtils.pivotMonthTableByBrgew)
      .transform(TransformUtils.mappingTableByMonth)
      .transform(TransformUtils.addSERNOByMonth)
  }
}