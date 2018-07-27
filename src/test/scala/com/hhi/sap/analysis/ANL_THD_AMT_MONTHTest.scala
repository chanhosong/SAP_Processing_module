package com.hhi.sap.analysis

import com.hhi.sap.table.term.TERM_MASTER
import com.hhi.sap.utils.SparkFileReader
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class ANL_THD_AMT_MONTHTest extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val INPUTPATH = "src/test/resources"
  private val FILEPATH_ZPDCT6123 = "/output/table3"
  private val FILEPATH_MARA = "/output/table5/*"
  private val FILEPATH_MARC = "/output/table6/*"
  private val FILEPATH_QBEW = "/output/table8/*"
  private val FILEPATH_MBEW = "/output/table9/*"

  "ZPDCT6123" should "be counted." in new SparkFileReader {
    println(getFolder(INPUTPATH+FILEPATH_ZPDCT6123).count())
  }

  "AMT_MONTH" should "make dataframe." in new SparkFileReader {
    //Please generate a table ZPDCT6123 on class ANL_THD_ZPSCT600_RTest
    val tb_ZPDCT6123 = ss.read.option("header", "true").csv(INPUTPATH + FILEPATH_ZPDCT6123)
    val tb_MARA = ss.read.option("header", "true").csv(INPUTPATH + FILEPATH_MARA)
    val tb_MARC = ss.read.option("header", "true").csv(INPUTPATH + FILEPATH_MARC)//Please create table6 by GEN_MARCTest class before running the app.
    val tb_QBEW = ss.read.option("header", "true").csv(INPUTPATH + FILEPATH_QBEW)
    val tb_MBEW = ss.read.option("header", "true").csv(INPUTPATH + FILEPATH_MBEW)

    new ANL_THD_AMT_MONTH(ss.sqlContext).run(
      tb_ZPDCT6123.select(TERM_MASTER.ZPDCT6123.COMPANYID, TERM_MASTER.ZPDCT6123.SAUPBU, TERM_MASTER.ZPDCT6123.PSPID, TERM_MASTER.ZPDCT6123.STG_GUBUN, TERM_MASTER.ZPDCT6123.MAT_GUBUN, TERM_MASTER.ZPDCT6123.IDNRK, TERM_MASTER.ZPDCT6123.MENGE, TERM_MASTER.ZPDCT6123.MEINS, TERM_MASTER.ZPDCT6123.WERKS, TERM_MASTER.ZPDCT6123.MONTH)
      , tb_MARA.select(TERM_MASTER.MARA.COMPANYID, TERM_MASTER.MARA.SAUPBU, TERM_MASTER.MARA.MATNR, TERM_MASTER.MARA.MEINS, TERM_MASTER.MARA.ZZMGROUP)
      , tb_MARC.select(TERM_MASTER.MARC.COMPANYID, TERM_MASTER.MARC.SAUPBU, TERM_MASTER.MARC.MATNR, TERM_MASTER.MARC.SBDKZ, TERM_MASTER.MARC.WERKS)
      , tb_QBEW.select(TERM_MASTER.QBEW.COMPANYID, TERM_MASTER.QBEW.SAUPBU, TERM_MASTER.QBEW.MATNR, TERM_MASTER.QBEW.VERPR, TERM_MASTER.QBEW.PEINH, TERM_MASTER.QBEW.WERKS, TERM_MASTER.QBEW.PSPID, TERM_MASTER.QBEW.SOBKZ, TERM_MASTER.QBEW.MATNR)
      , tb_MBEW.select(TERM_MASTER.MBEW.COMPANYID, TERM_MASTER.MBEW.SAUPBU, TERM_MASTER.MBEW.MATNR, TERM_MASTER.MBEW.VERPR, TERM_MASTER.MBEW.PEINH, TERM_MASTER.MBEW.WERKS, TERM_MASTER.MBEW.MATNR))
      .show()
  }
}