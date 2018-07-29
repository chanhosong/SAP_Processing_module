package com.hhi.sap.analysis.daily

import com.hhi.sap.analysis.functions.ProcessClassification
import com.hhi.sap.analysis.functions.common.DateTimeUtil
import com.hhi.sap.table.bean.BEAN_ZPDCT6023
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class ANL_ZPDCT6023(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(tb_ZPDCT6023: DataFrame, tb_ZPSCT_600: DataFrame, eban: DataFrame, mara: DataFrame): DataFrame = genTable(tb_ZPDCT6023, tb_ZPSCT_600, eban, mara)

  private def genTable(tb_ZPDCT6023: DataFrame, tb_ZPSCT_600: DataFrame, eban: DataFrame, mara: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    var PGMID = "Spark2.3.0.cloudera2"
    var CNAM = "A504863"

    if (logger.isDebugEnabled()) {
      PGMID = "[DEBUGMODE]Spark2.3.0.cloudera2"
      CNAM = "[DEBUGMODE]A504863"
    }

    loadTable(tb_ZPDCT6023, tb_ZPSCT_600, eban, mara).rdd
      .map(e=>
        BEAN_ZPDCT6023(
          e.getAs(TERM_MASTER.ZPDCT6023.COMPANYID),
          e.getAs(TERM_MASTER.ZPDCT6023.SAUPBU),
          e.getAs(TERM_MASTER.ZPDCT6023.ZTRKNO),
          e.getAs(TERM_MASTER.ZPDCT6023.ZREVNO),
          e.getAs(TERM_MASTER.ZPDCT6023.MATNR),
          e.getAs(TERM_MASTER.ZPDCT6023.POSNR),
          e.getAs(TERM_MASTER.ZPDCT6023.IDNRK),
          e.getAs(TERM_MASTER.ZPDCT6023.ZINOUT),
          e.getAs(TERM_MASTER.ZPDCT6023.PSPID),
          e.getAs(TERM_MASTER.ZPDCT6023.ZIDWGNO),
          e.getAs(TERM_MASTER.ZPDCT6023.ZMDWGNO),
          e.getAs(TERM_MASTER.ZPDCT6023.ZMIDACTNO),
          e.getAs(TERM_MASTER.ZPDCT6023.MENGE),
          e.getAs(TERM_MASTER.ZPDCT6023.MEINS),
          e.getAs(TERM_MASTER.ZPDCT6023.BRGEW),
          e.getAs(TERM_MASTER.ZPDCT6023.ZBLKNO),
          e.getAs(TERM_MASTER.ZPDCT6023.ZPCSNO),
          e.getAs(TERM_MASTER.ZPDCT6023.ZKGPORNO),
          e.getAs(TERM_MASTER.ZPDCT6023.ZKGBNFPO),
          e.getAs(TERM_MASTER.ZPDCT6023.ZHDRMATNR),
          e.getAs(TERM_MASTER.ZPDCT6023.BANFN),
          e.getAs(TERM_MASTER.ZPDCT6023.BFNPO),
          e.getAs(TERM_MASTER.ZPDCT6023.ZCNFDATE),
          e.getAs(TERM_MASTER.ZPDCT6023.ZFROMSYS),
          e.getAs(TERM_MASTER.ZPDCT6023.ZPTMR),
          e.getAs(TERM_MASTER.ZPDCT6023.ZMRPL),
          e.getAs(TERM_MASTER.ZPDCT6023.WERKS),
          DateTimeUtil.getWeekDifference(e.getAs(TERM_MASTER.ZPDCT6023.ZCNFDATE), e.getAs(TERM_MASTER.ZPSCT600.WC)).toString,
          DateTimeUtil.getMonthDifference(e.getAs(TERM_MASTER.ZPDCT6023.ZCNFDATE), e.getAs(TERM_MASTER.ZPSCT600.WC)).toString,
          ProcessClassification.getSTG_GUBUN(e.getAs(TERM_MASTER.ZPDCT6023.ZMIDACTNO), e.getAs(TERM_MASTER.ZPDCT6023.ZHDRMATNR)),
          ProcessClassification.getMAT_GUBUN(e.getAs(TERM_MASTER.ZPDCT6023.ZMIDACTNO), e.getAs(TERM_MASTER.EBAN.LGORT), e.getAs(TERM_MASTER.EBAN.PAINTGBN), e.getAs(TERM_MASTER.MARA.ZZMGROUP)),
          PGMID,
          CNAM,
          DateTimeUtil.date,
          DateTimeUtil.time
        )
      ).toDF
  }

  private def loadTable(tb_ZPDCT6023: DataFrame, tb_ZPSCT_600: DataFrame, eban: DataFrame, mara: DataFrame): DataFrame = {

    tb_ZPDCT6023.select(
      TERM_MASTER.ZPDCT6023.COMPANYID,
      TERM_MASTER.ZPDCT6023.SAUPBU,
      TERM_MASTER.ZPDCT6023.ZTRKNO,
      TERM_MASTER.ZPDCT6023.ZREVNO,
      TERM_MASTER.ZPDCT6023.MATNR,
      TERM_MASTER.ZPDCT6023.POSNR,
      TERM_MASTER.ZPDCT6023.IDNRK,
      TERM_MASTER.ZPDCT6023.ZINOUT,
      TERM_MASTER.ZPDCT6023.PSPID,
      TERM_MASTER.ZPDCT6023.ZIDWGNO,
      TERM_MASTER.ZPDCT6023.ZMDWGNO,
      TERM_MASTER.ZPDCT6023.ZMIDACTNO,
      TERM_MASTER.ZPDCT6023.MENGE,
      TERM_MASTER.ZPDCT6023.MEINS,
      TERM_MASTER.ZPDCT6023.BRGEW,
      TERM_MASTER.ZPDCT6023.ZBLKNO,
      TERM_MASTER.ZPDCT6023.ZPCSNO,
      TERM_MASTER.ZPDCT6023.ZKGPORNO,
      TERM_MASTER.ZPDCT6023.ZKGBNFPO,
      TERM_MASTER.ZPDCT6023.ZHDRMATNR,
      TERM_MASTER.ZPDCT6023.BANFN,
      TERM_MASTER.ZPDCT6023.BFNPO,
      TERM_MASTER.ZPDCT6023.ZCNFDATE,
      TERM_MASTER.ZPDCT6023.ZFROMSYS,
      TERM_MASTER.ZPDCT6023.ZPTMR,
      TERM_MASTER.ZPDCT6023.ZMRPL,
      TERM_MASTER.ZPDCT6023.WERKS
    ).join(tb_ZPSCT_600, Seq(TERM_MASTER.ZPSCT600.COMPANYID, TERM_MASTER.ZPSCT600.SAUPBU, TERM_MASTER.ZPSCT600.PSPID))
      .join(eban, Seq(TERM_MASTER.EBAN.COMPANYID, TERM_MASTER.EBAN.SAUPBU, TERM_MASTER.EBAN.BANFN, TERM_MASTER.EBAN.BFNPO))
      .join(mara, Seq(TERM_MASTER.MARA.COMPANYID, TERM_MASTER.MARA.SAUPBU, TERM_MASTER.MARA.MATNR))
  }
}
