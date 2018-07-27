package com.hhi.sap.analysis

import com.hhi.sap.analysis.functions.AMTTableUtils
import com.hhi.sap.analysis.functions.common.TransformUtils
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class ANL_THD_AMT_WEEK(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(zpdct6123: DataFrame, mara: DataFrame, marc: DataFrame, qbew: DataFrame, mbew: DataFrame): DataFrame = genTable(zpdct6123, mara, marc, qbew, mbew)

  private def genTable(zpdct6123: DataFrame, mara: DataFrame, marc: DataFrame, qbew: DataFrame, mbew: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    val joinedRDD1 = zpdct6123.as(TERM_MASTER.ZPDCT6123.TABLENAME)
      .join(mara.as(TERM_MASTER.MARA.TABLENAME), zpdct6123(TERM_MASTER.ZPDCT6123.COMPANYID) <=> mara(TERM_MASTER.MARA.COMPANYID) && zpdct6123(TERM_MASTER.ZPDCT6123.SAUPBU) <=> mara(TERM_MASTER.MARA.SAUPBU) && zpdct6123(TERM_MASTER.ZPDCT6123.IDNRK) <=> mara(TERM_MASTER.MARA.MATNR), "left")
      .join(marc.as(TERM_MASTER.MARC.TABLENAME), zpdct6123(TERM_MASTER.ZPDCT6123.COMPANYID) <=> marc(TERM_MASTER.MARC.COMPANYID) && zpdct6123(TERM_MASTER.ZPDCT6123.SAUPBU) <=> marc(TERM_MASTER.MARC.SAUPBU) /*&& zpdct6123(TERM_MASTER.ZPDCT6123.WERKS) <=> marc(TERM_MASTER.MARC.WERKS)*/ && zpdct6123(TERM_MASTER.ZPDCT6123.IDNRK) <=> marc(TERM_MASTER.MARC.MATNR), "left")
      .where(s"${TERM_MASTER.MARC.TABLENAME}.${TERM_MASTER.MARC.SBDKZ} = 1")
      .join(qbew.as(TERM_MASTER.QBEW.TABLENAME), zpdct6123(TERM_MASTER.ZPDCT6123.COMPANYID) <=> qbew(TERM_MASTER.QBEW.COMPANYID) && zpdct6123(TERM_MASTER.ZPDCT6123.SAUPBU) <=> qbew(TERM_MASTER.QBEW.SAUPBU) && zpdct6123(TERM_MASTER.ZPDCT6123.PSPID) <=> qbew(TERM_MASTER.QBEW.PSPID) /*&& zpdct6123(TERM_MASTER.ZPDCT6123.WERKS) <=> qbew(TERM_MASTER.QBEW.WERKS)*/  && zpdct6123(TERM_MASTER.ZPDCT6123.IDNRK) <=> qbew(TERM_MASTER.QBEW.MATNR), "left")
      .where(s"${TERM_MASTER.QBEW.TABLENAME}.${TERM_MASTER.QBEW.SOBKZ} = 'Q'")
      .where(s"${TERM_MASTER.QBEW.TABLENAME}.${TERM_MASTER.QBEW.VERPR} != 0")
      .select(
        s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.COMPANYID}"
        ,s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.SAUPBU}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.PSPID}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.STG_GUBUN}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.MAT_GUBUN}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.MENGE}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.MEINS}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.WERKS}"
        , s"${TERM_MASTER.MARA.TABLENAME}.${TERM_MASTER.MARA.ZZMGROUP}"
        , s"${TERM_MASTER.MARC.TABLENAME}.${TERM_MASTER.MARC.SBDKZ}"
        , s"${TERM_MASTER.QBEW.TABLENAME}.${TERM_MASTER.QBEW.VERPR}"
        , s"${TERM_MASTER.QBEW.TABLENAME}.${TERM_MASTER.QBEW.PEINH}"
        , s"${TERM_MASTER.QBEW.TABLENAME}.${TERM_MASTER.QBEW.SOBKZ}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.WEEK}"
      ).toDF(
      TERM_MASTER.ZPDCT6123.COMPANYID,
      TERM_MASTER.ZPDCT6123.SAUPBU,
      TERM_MASTER.ZPDCT6123.PSPID,
      TERM_MASTER.ZPDCT6123.STG_GUBUN,
      TERM_MASTER.ZPDCT6123.MAT_GUBUN,
      TERM_MASTER.ZPDCT6123.MENGE,
      TERM_MASTER.ZPDCT6123.MEINS,
      TERM_MASTER.ZPDCT6123.WERKS,
      TERM_MASTER.MARA.ZZMGROUP,
      TERM_MASTER.MARC.SBDKZ,
      TERM_MASTER.QBEW.VERPR,
      TERM_MASTER.QBEW.PEINH,
      TERM_MASTER.QBEW.SOBKZ,
      TERM_MASTER.ZPDCT6123.WEEK
    )

    val joinedRDD2 = zpdct6123.as(TERM_MASTER.ZPDCT6123.TABLENAME)
      .join(mara.as(TERM_MASTER.MARA.TABLENAME), zpdct6123(TERM_MASTER.ZPDCT6123.COMPANYID) <=> mara(TERM_MASTER.MARA.COMPANYID) && zpdct6123(TERM_MASTER.ZPDCT6123.SAUPBU) <=> mara(TERM_MASTER.MARA.SAUPBU) && zpdct6123(TERM_MASTER.ZPDCT6123.IDNRK) <=> mara(TERM_MASTER.MARA.MATNR), "left")
      .join(marc.as(TERM_MASTER.MARC.TABLENAME), zpdct6123(TERM_MASTER.ZPDCT6123.COMPANYID) <=> marc(TERM_MASTER.MARC.COMPANYID) && zpdct6123(TERM_MASTER.ZPDCT6123.SAUPBU) <=> marc(TERM_MASTER.MARC.SAUPBU) /*&& zpdct6123(TERM_MASTER.ZPDCT6123.WERKS) <=> marc(TERM_MASTER.MARC.WERKS)*/ && zpdct6123(TERM_MASTER.ZPDCT6123.IDNRK) <=> marc(TERM_MASTER.MARC.MATNR), "left")
      .where(s"${TERM_MASTER.MARC.TABLENAME}.${TERM_MASTER.MARC.SBDKZ} = 2")
      .join(mbew.as(TERM_MASTER.MBEW.TABLENAME), zpdct6123(TERM_MASTER.ZPDCT6123.COMPANYID) <=> mbew(TERM_MASTER.MBEW.COMPANYID) && zpdct6123(TERM_MASTER.ZPDCT6123.SAUPBU) <=> mbew(TERM_MASTER.MBEW.SAUPBU) /*&& zpdct6123(TERM_MASTER.ZPDCT6123.WERKS) <=> mbew(TERM_MASTER.MBEW.WERKS)*/  && zpdct6123(TERM_MASTER.ZPDCT6123.IDNRK) <=> mbew(TERM_MASTER.MBEW.MATNR), "left")
      .where(s"${TERM_MASTER.MBEW.TABLENAME}.${TERM_MASTER.MBEW.VERPR} != 0")
      .select(
        s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.COMPANYID}"
        ,s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.SAUPBU}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.PSPID}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.STG_GUBUN}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.MAT_GUBUN}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.MENGE}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.MEINS}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.WERKS}"
        , s"${TERM_MASTER.MARA.TABLENAME}.${TERM_MASTER.MARA.ZZMGROUP}"
        , s"${TERM_MASTER.MARC.TABLENAME}.${TERM_MASTER.MARC.SBDKZ}"
        , s"${TERM_MASTER.MBEW.TABLENAME}.${TERM_MASTER.MBEW.VERPR}"
        , s"${TERM_MASTER.MBEW.TABLENAME}.${TERM_MASTER.MBEW.PEINH}"
        , s"${TERM_MASTER.ZPDCT6123.TABLENAME}.${TERM_MASTER.ZPDCT6123.WEEK}"
      ).toDF(
      TERM_MASTER.ZPDCT6123.COMPANYID,
      TERM_MASTER.ZPDCT6123.SAUPBU,
      TERM_MASTER.ZPDCT6123.PSPID,
      TERM_MASTER.ZPDCT6123.STG_GUBUN,
      TERM_MASTER.ZPDCT6123.MAT_GUBUN,
      TERM_MASTER.ZPDCT6123.MENGE,
      TERM_MASTER.ZPDCT6123.MEINS,
      TERM_MASTER.ZPDCT6123.WERKS,
      TERM_MASTER.MARA.ZZMGROUP,
      TERM_MASTER.MARC.SBDKZ,
      TERM_MASTER.MBEW.VERPR,
      TERM_MASTER.MBEW.PEINH,
      TERM_MASTER.ZPDCT6123.WEEK
    )

    val amtRDD = AMTTableUtils.getAMTRDDByWeek(joinedRDD1).union(AMTTableUtils.getAMTRDDByWeek(joinedRDD2))
    val underDF = AMTTableUtils.getWeekTable(amtRDD.filter(_.week <= -5), -5)
    val upperDF = AMTTableUtils.getWeekTable(amtRDD.filter(_.week >= 25), 25)

    TransformUtils
      .makeUnion(amtRDD.filter(-4 until 24 contains _.week).toDF(), underDF, upperDF)
      .transform(TransformUtils.pivotWeekTableByAmount)
      .transform(TransformUtils.mappingTableByWeek)
      .transform(TransformUtils.addSERNOByWeek)
  }
}