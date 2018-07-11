package com.hhi.sap.analysis

import com.hhi.sap.table.term.TERM_MASTER
import com.hhi.sap.utils.SparkFileReader
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class ANL_THD_WEIGHT_WEEKTest extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val INPUTPATH = "src/test/resources"
  private val TABLE4 = "/output/table4"
  private val FILENPATH_ZPSCT600 = "/ZPDCV6021/*"
  private val FILENPATH_ZPDCT6023 = "/ZPDCT6023/*"
  private val FILEPATH_EBAN = "/EBAN/*"
  private val FILEPATH_MARA = "/MARA/*"

  "ZPDCT6023" should "be counted." in new SparkFileReader {
    println(getFolder(INPUTPATH+FILENPATH_ZPDCT6023).count())
  }

  "ZPDCT6023 " should "make dataframe." in new SparkFileReader {
    //"Please be generated a table ZPDCT6023 on class ANL_THD_ZPSCT600_RTest "
    val tb_ZPDCT6023 = ss.read.option("header", "true").csv(INPUTPATH + TABLE4)

    new ANL_THD_MRPL_WEEK(ss.sqlContext).run(tb_ZPDCT6023
      .select(TERM_MASTER.ZPDCT6023.COMPANYID, TERM_MASTER.ZPDCT6023.SAUPBU, TERM_MASTER.ZPDCT6023.ZTRKNO, TERM_MASTER.ZPDCT6023.ZREVNO, TERM_MASTER.ZPDCT6023.PSPID, TERM_MASTER.ZPDCT6023.STG_GUBUN, TERM_MASTER.ZPDCT6023.MAT_GUBUN, TERM_MASTER.ZPDCT6023.WEEK))
      .show()
  }
}