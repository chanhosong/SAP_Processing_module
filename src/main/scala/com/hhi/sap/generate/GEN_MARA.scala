package com.hhi.sap.generate

import com.hhi.sap.config.DateTimeUtil
import com.hhi.sap.table.bean.BEAN_MARA
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class GEN_MARA(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(mara: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    var PGMID = "Spark2.3.0.cloudera2"
    var CNAM = "A504863"

    if (logger.isDebugEnabled) {
      PGMID = "[DEBUGMODE]Spark2.3.0.cloudera2"
      CNAM = "[DEBUGMODE]A504863"
    }

    mara.rdd.map(e=>{
      BEAN_MARA(
        e.getAs(TERM_MASTER.MARA.COMPANYID),
        e.getAs(TERM_MASTER.MARA.SAUPBU),
        e.getAs(TERM_MASTER.MARA.MATNR),
        e.getAs(TERM_MASTER.MARA.MTART),
        e.getAs(TERM_MASTER.MARA.MATKL),
        e.getAs(TERM_MASTER.MARA.MEINS),
        e.getAs(TERM_MASTER.MARA.BRGEW),
        e.getAs(TERM_MASTER.MARA.NTGEW),
        e.getAs(TERM_MASTER.MARA.GEWEI),
        e.getAs(TERM_MASTER.MARA.XCHPF),
        e.getAs(TERM_MASTER.MARA.ZZMAKTX1),
        e.getAs(TERM_MASTER.MARA.ZZMGROUP),
        e.getAs(TERM_MASTER.MARA.ZZDGIOX),
        e.getAs(TERM_MASTER.MARA.ZZLEN),
        e.getAs(TERM_MASTER.MARA.ERSDA),
        e.getAs(TERM_MASTER.MARA.LAEDA),
        PGMID,
        CNAM,
        DateTimeUtil.date,
        DateTimeUtil.time
      )
    }).toDF()
  }
}