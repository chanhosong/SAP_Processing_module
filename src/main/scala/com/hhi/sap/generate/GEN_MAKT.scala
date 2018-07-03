package com.hhi.sap.generate

import com.hhi.sap.config.DateTimeUtil
import com.hhi.sap.table.bean.BEAN_MAKT
import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

class GEN_MAKT(sql: SQLContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(makt: DataFrame): DataFrame = {
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

    makt.rdd.map(e=>{
      BEAN_MAKT(
        e.getAs(TERM_MASTER.MAKT.COMPANYID),
        e.getAs(TERM_MASTER.MAKT.SAUPBU),
        e.getAs(TERM_MASTER.MAKT.MATNR),
        e.getAs(TERM_MASTER.MAKT.MAKTX),
        PGMID,
        CNAM,
        date,
        time
      )
    }).toDF()
  }
}