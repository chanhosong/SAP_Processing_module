package com.hhi.sap.generate

import com.hhi.sap.config.DateTimeUtil
import com.hhi.sap.table.bean.BEAN_MARC
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class GEN_MARC(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(marc: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    var PGMID = "Spark2.3.0.cloudera2"
    var CNAM = "A504863"

    if (logger.isDebugEnabled) {
      PGMID = "[DEBUGMODE]Spark2.3.0.cloudera2"
      CNAM = "[DEBUGMODE]A504863"
    }

    marc.rdd.map(e=>{
      BEAN_MARC(
        e.getAs(TERM_MASTER.MARC.COMPANYID),
        e.getAs(TERM_MASTER.MARC.SAUPBU),
        e.getAs(TERM_MASTER.MARC.MATNR),
        e.getAs(TERM_MASTER.MARC.WERKS),
        e.getAs(TERM_MASTER.MARC.EKGRP),
        e.getAs(TERM_MASTER.MARC.DISMM),
        e.getAs(TERM_MASTER.MARC.SBDKZ),
        PGMID,
        CNAM,
        DateTimeUtil.date,
        DateTimeUtil.time
      )
    }).toDF()
  }
}