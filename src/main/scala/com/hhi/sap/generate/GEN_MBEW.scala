package com.hhi.sap.generate

import com.hhi.sap.config.DateTimeUtil
import com.hhi.sap.table.bean.BEAN_MBEW
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class GEN_MBEW(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(mbew: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._

    var PGMID = "Spark2.3.0.cloudera2"
    var CNAM = "A504863"

    if (logger.isWarnEnabled) {
      PGMID = "[DEBUGMODE]Spark2.3.0.cloudera2"
      CNAM = "[DEBUGMODE]A504863"
    }
    val dtu = new DateTimeUtil()

    val date = dtu.date
    val time = dtu.time

    mbew.rdd.map(e=>{
      BEAN_MBEW(
        e.getAs(TERM_MASTER.MBEW.COMPANYID),
        e.getAs(TERM_MASTER.MBEW.SAUPBU),
        e.getAs(TERM_MASTER.MBEW.MATNR),
        e.getAs(TERM_MASTER.MBEW.WERKS),
        e.getAs(TERM_MASTER.MBEW.VERPR),
        e.getAs(TERM_MASTER.MBEW.PEINH),
        PGMID,
        CNAM,
        date,
        time
      )
    }).toDF()
  }
}