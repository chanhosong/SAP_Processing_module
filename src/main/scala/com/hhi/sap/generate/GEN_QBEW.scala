package com.hhi.sap.generate

import com.hhi.sap.config.DateTimeUtil
import com.hhi.sap.table.bean.BEAN_QBEW
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class GEN_QBEW(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(qbew: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    var PGMID = "Spark2.3.0.cloudera2"
    var CNAM = "A504863"

    if (logger.isDebugEnabled) {
      PGMID = "[DEBUGMODE]Spark2.3.0.cloudera2"
      CNAM = "[DEBUGMODE]A504863"
    }

    qbew.rdd.map(e=>{
      BEAN_QBEW(
        e.getAs(TERM_MASTER.QBEW.COMPANYID),
        e.getAs(TERM_MASTER.QBEW.SAUPBU),
        e.getAs(TERM_MASTER.QBEW.MATNR),
        e.getAs(TERM_MASTER.QBEW.WERKS),
        e.getAs(TERM_MASTER.QBEW.SOBKZ),
        e.getAs(TERM_MASTER.QBEW.PSPID),
        e.getAs(TERM_MASTER.QBEW.VERPR),
        e.getAs(TERM_MASTER.QBEW.PEINH),
        PGMID,
        CNAM,
        DateTimeUtil.date,
        DateTimeUtil.time
      )
    }).toDF()
  }
}