package com.hhi.sap.analysis.monthly

import com.hhi.sap.table.term.TERM_MASTER
import com.hhi.sap.utils.SparkFileReader
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class ANL_THD_MRPL_MONTHTest extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val INPUTPATH = "src/test/resources"
  private val TABLE3 = "/output/table3"
  private val FILENPATH_ZPSCT600 = "/ZPDCV6021/*"
  private val FILENPATH_ZPDCT6123 = "/ZPDCT6123/*"

  "ZPDCT6123" should "be counted." in new SparkFileReader {
    println(getFolder(INPUTPATH+FILENPATH_ZPDCT6123).count())
  }

  "MRPL_MONTH" should "make dataframe." in new SparkFileReader {
    //Please generate a table ZPDCT6123 on class ANL_THD_ZPSCT600_RTest
    val tb_ZPDCT6123 = ss.read.option("header", "true").csv(INPUTPATH + TABLE3)

    new ANL_THD_MRPL_MONTH(ss.sqlContext).run(
      tb_ZPDCT6123.select(TERM_MASTER.ZPDCT6123.COMPANYID, TERM_MASTER.ZPDCT6123.SAUPBU, TERM_MASTER.ZPDCT6123.PSPID, TERM_MASTER.ZPDCT6123.STG_GUBUN, TERM_MASTER.ZPDCT6123.MAT_GUBUN, TERM_MASTER.ZPDCT6123.MONTH))
      .show()
  }
}
