package com.hhi.sap.main

import com.hhi.sap.analysis.{ANL_THD_MRPL_WEEK, ANL_THD_ZPSCT600_R, ANL_ZPDCT6023, ANL_ZPDCT6123}
import com.hhi.sap.generate._
import com.hhi.sap.table.term.TERM_MASTER
import org.slf4j.{Logger, LoggerFactory}


object Main extends App with SparkSessionWrapper{
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val HDFS_PATH = "/user/sap/hive/table_raw/SAP/"
  private val FACTOR_PATH = "FACTOR"
  private val ZPDCV6021_PATH = "ZPDCV6021"
  private val ZPDCT6123_PATH = "ZPDCT6123"
  private val ZPDCT6023_PATH = "ZPDCT6023"
  private val EBAN_PATH = "EBAN"
  private val MARA_PATH = "MARA"
  private val MARC_PATH = "MARC"
  private val MAKT_PATH = "MAKT"
  private val QBEW_PATH = "QBEW"
  private val MBEW_PATH = "MBEW"

  if (logger.isDebugEnabled()) {
    logger.debug("Start debugging for main.")
    logger.debug("This App will be running spark on local[*].")
  }

  /*Loading the table*/
//  val tb_FACTORMASTER = ss.sqlContext.sql(SQL_MASTER.FACTOR.SQL_INQUIRE_FACTOR)
//  val tb_ZPDCV6021 = ss.sqlContext.sql(SQL_MASTER.ZPDCV6021.SQL_ALLDATA_THD_ZPDCV6021)
//  val tb_ZPDCT6123 = ss.sqlContext.sql(SQL_MASTER.ZPDCT6123.SQL_ALLDATA_THD_ZPDCT6123)
//  val tb_ZPDCT6023 = ss.sqlContext.sql(SQL_MASTER.ZPDCT6023.SQL_ALLDATA_THD_ZPDCT6023)
//  val tb_EBAN = ss.sqlContext.sql(SQL_MASTER.EBAN.SQL_ALLDATA_EBAN)
//  val tb_MARA = ss.sqlContext.sql(SQL_MASTER.MARA.SQL_ALLDATA_MARA)
//  val tb_MARC = ss.sqlContext.sql(SQL_MASTER.MARC.SQL_ALLDATA_MARC)
//  val tb_MAKT = ss.sqlContext.sql(SQL_MASTER.MAKT.SQL_ALLDATA_MAKT)
//  val tb_QBEW = ss.sqlContext.sql(SQL_MASTER.QBEW.SQL_ALLDATA_QBEW)
//  val tb_MBEW = ss.sqlContext.sql(SQL_MASTER.MBEW.SQL_ALLDATA_MBEW)
//  val tb_MRPL_WEEK = ss.sqlContext.sql(SQL_MASTER.MRPL_WEEK.SQL_ALLDATA_MRPL_WEEK)

  val tb_FACTORMASTER = ss.read.option("header", "true").csv(HDFS_PATH+FACTOR_PATH)
  val tb_ZPSCT600 = ss.read.option("header", "true").csv(HDFS_PATH+ZPDCV6021_PATH)
  val tb_ZPDCT6123 = ss.read.option("header", "true").csv(HDFS_PATH+ZPDCT6123_PATH)
  val tb_ZPDCT6023 = ss.read.option("header", "true").csv(HDFS_PATH+ZPDCT6023_PATH)
  val tb_EBAN = ss.read.option("header", "true").csv(HDFS_PATH+EBAN_PATH)
  val tb_MARA = ss.read.option("header", "true").csv(HDFS_PATH+MARA_PATH)
  val tb_MARC = ss.read.option("header", "true").csv(HDFS_PATH+MARC_PATH)
  val tb_MAKT = ss.read.option("header", "true").csv(HDFS_PATH+MAKT_PATH)
  val tb_QBEW = ss.read.option("header", "true").csv(HDFS_PATH+QBEW_PATH)
  val tb_MBEW = ss.read.option("header", "true").csv(HDFS_PATH+MBEW_PATH)

  /*Analysis the table and generate it.*/
  //1,2: Daily
  val df_THD_ZPSCT600_R = new ANL_THD_ZPSCT600_R(ss.sqlContext).run(tb_ZPSCT600, tb_FACTORMASTER)

  //3,4: Daily
  val df_ZPDCT6123 = new ANL_ZPDCT6123(ss.sqlContext).run(tb_ZPDCT6123, df_THD_ZPSCT600_R, tb_EBAN.select(TERM_MASTER.EBAN.BANFN, TERM_MASTER.EBAN.BFNPO, TERM_MASTER.EBAN.LGORT, TERM_MASTER.EBAN.PAINTGBN), tb_MARA.select(TERM_MASTER.MARA.MATNR, TERM_MASTER.MARA.ZZMGROUP)).show(1000)
  val df_ZPDCT6023 = new ANL_ZPDCT6023(ss.sqlContext).run(tb_ZPDCT6023, df_THD_ZPSCT600_R, tb_EBAN.select(TERM_MASTER.EBAN.BANFN, TERM_MASTER.EBAN.BFNPO, TERM_MASTER.EBAN.LGORT, TERM_MASTER.EBAN.PAINTGBN), tb_MARA.select(TERM_MASTER.MARA.MATNR, TERM_MASTER.MARA.ZZMGROUP)).show(1000)

  //5,6,7,8,9: Daily
  val df_MARA = new GEN_MARA(ss.sqlContext).run(tb_MARA)
  val df_MARC = new GEN_MARC(ss.sqlContext).run(tb_MARC)
  val df_MAKT = new GEN_MAKT(ss.sqlContext).run(tb_MAKT)
  val df_QBEW = new GEN_QBEW(ss.sqlContext).run(tb_QBEW)
  val df_MBEW = new GEN_MBEW(ss.sqlContext).run(tb_MBEW)

  //10,11,12: Weekly
  val df_MRPL_WEEK = new ANL_THD_MRPL_WEEK(ss.sqlContext).run(tb_ZPDCT6023.select(TERM_MASTER.ZPDCT6023.COMPANYID, TERM_MASTER.ZPDCT6023.SAUPBU, TERM_MASTER.ZPDCT6023.ZTRKNO, TERM_MASTER.ZPDCT6023.ZREVNO, TERM_MASTER.ZPDCT6023.PSPID, TERM_MASTER.ZPDCT6023.STG_GUBUN, TERM_MASTER.ZPDCT6023.MAT_GUBUN, TERM_MASTER.ZPDCT6023.WEEK))


  /*End Application*/
  ss.stop()
}