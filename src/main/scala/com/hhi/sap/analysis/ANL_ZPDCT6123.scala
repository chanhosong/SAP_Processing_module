package com.hhi.sap.analysis

import com.hhi.sap.analysis.functions.ProcessClassification
import com.hhi.sap.config.DateTimeUtil
import com.hhi.sap.table.bean.BEAN_ZPDCT6123
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class ANL_ZPDCT6123(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(tb_ZPDCT6123: DataFrame, tb_ZPSCT_600: DataFrame, eban: DataFrame, mara: DataFrame): DataFrame = genTable(tb_ZPDCT6123, tb_ZPSCT_600, eban, mara)

  private def genTable(tb_ZPDCT6123: DataFrame, tb_ZPSCT_600: DataFrame, eban: DataFrame, mara: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    var PGMID = "Spark2.3.0.cloudera2"
    var CNAM = "A504863"

    if (logger.isDebugEnabled()) {
      PGMID = "[DEBUGMODE]Spark2.3.0.cloudera2"
      CNAM = "[DEBUGMODE]A504863"
    }

    loadTable(tb_ZPDCT6123, tb_ZPSCT_600, eban, mara).rdd
      .map(e=>
        BEAN_ZPDCT6123(
          e.getAs(TERM_MASTER.ZPDCT6123.COMPANYID),
          e.getAs(TERM_MASTER.ZPDCT6123.SAUPBU),
          e.getAs(TERM_MASTER.ZPDCT6123.MATNR),
          e.getAs(TERM_MASTER.ZPDCT6123.POSNR),
          e.getAs(TERM_MASTER.ZPDCT6123.IDNRK),
          e.getAs(TERM_MASTER.ZPDCT6123.ZINOUT),
          e.getAs(TERM_MASTER.ZPDCT6123.PSPID),
          e.getAs(TERM_MASTER.ZPDCT6123.ZMRPL),
          e.getAs(TERM_MASTER.ZPDCT6123.ZKGPORNO),
          e.getAs(TERM_MASTER.ZPDCT6123.ZKGBNFPO),
          e.getAs(TERM_MASTER.ZPDCT6123.ZMIDACTNO),
          e.getAs(TERM_MASTER.ZPDCT6123.BANFN),
          e.getAs(TERM_MASTER.ZPDCT6123.BFNPO),
          e.getAs(TERM_MASTER.ZPDCT6123.ZHDRMATNR),
          e.getAs(TERM_MASTER.ZPDCT6123.ZEXDATE),
          e.getAs(TERM_MASTER.ZPDCT6123.MENGE),
          e.getAs(TERM_MASTER.ZPDCT6123.ISSQTY),
          e.getAs(TERM_MASTER.ZPDCT6123.MEINS),
          e.getAs(TERM_MASTER.ZPDCT6123.ZKGVNDCOD),
          e.getAs(TERM_MASTER.ZPDCT6123.ZBLKNO),
          e.getAs(TERM_MASTER.ZPDCT6123.WERKS),
          e.getAs(TERM_MASTER.ZPDCT6123.ZFROMSYS),
          e.getAs(TERM_MASTER.ZPDCT6123.ZPTMR),
          DateTimeUtil.getWeekDifference(e.getAs(TERM_MASTER.ZPDCT6123.ZEXDATE), e.getAs(TERM_MASTER.ZPSCT600.WC)).toString,
          DateTimeUtil.getMonthDifference(e.getAs(TERM_MASTER.ZPDCT6123.ZEXDATE), e.getAs(TERM_MASTER.ZPSCT600.WC)).toString,
          ProcessClassification.getSTG_GUBUN(e.getAs(TERM_MASTER.ZPDCT6123.ZMIDACTNO), e.getAs(TERM_MASTER.ZPDCT6123.ZHDRMATNR)),
          ProcessClassification.getMAT_GUBUN(e.getAs(TERM_MASTER.ZPDCT6123.ZMIDACTNO), e.getAs(TERM_MASTER.EBAN.LGORT), e.getAs(TERM_MASTER.EBAN.PAINTGBN), e.getAs(TERM_MASTER.MARA.ZZMGROUP)),
          PGMID,
          CNAM,
          DateTimeUtil.date,
          DateTimeUtil.time
        )
    ).toDF
  }

  private def loadTable(tb_ZPDCT6123: DataFrame, tb_ZPSCT_600: DataFrame, eban: DataFrame, mara: DataFrame): DataFrame = {
    tb_ZPDCT6123.select(
      TERM_MASTER.ZPDCT6123.COMPANYID,
      TERM_MASTER.ZPDCT6123.SAUPBU,
      TERM_MASTER.ZPDCT6123.MATNR,
      TERM_MASTER.ZPDCT6123.POSNR,
      TERM_MASTER.ZPDCT6123.IDNRK,
      TERM_MASTER.ZPDCT6123.ZINOUT,
      TERM_MASTER.ZPDCT6123.PSPID,
      TERM_MASTER.ZPDCT6123.ZMRPL,
      TERM_MASTER.ZPDCT6123.ZKGPORNO,
      TERM_MASTER.ZPDCT6123.ZKGBNFPO,
      TERM_MASTER.ZPDCT6123.ZMIDACTNO,
      TERM_MASTER.ZPDCT6123.BANFN,
      TERM_MASTER.ZPDCT6123.BFNPO,
      TERM_MASTER.ZPDCT6123.ZHDRMATNR,
      TERM_MASTER.ZPDCT6123.ZEXDATE,
      TERM_MASTER.ZPDCT6123.MENGE,
      TERM_MASTER.ZPDCT6123.ISSQTY,
      TERM_MASTER.ZPDCT6123.MEINS,
      TERM_MASTER.ZPDCT6123.ZKGVNDCOD,
      TERM_MASTER.ZPDCT6123.ZBLKNO,
      TERM_MASTER.ZPDCT6123.WERKS,
      TERM_MASTER.ZPDCT6123.ZFROMSYS,
      TERM_MASTER.ZPDCT6123.ZPTMR
    ).join(tb_ZPSCT_600, Seq(TERM_MASTER.ZPSCT600.COMPANYID, TERM_MASTER.ZPSCT600.SAUPBU, TERM_MASTER.ZPSCT600.PSPID))
      .join(eban, Seq(TERM_MASTER.EBAN.BANFN, TERM_MASTER.EBAN.BFNPO))
      .join(mara, Seq(TERM_MASTER.MARA.MATNR))
  }
}