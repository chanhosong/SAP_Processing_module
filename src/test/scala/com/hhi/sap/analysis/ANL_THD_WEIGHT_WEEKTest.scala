package com.hhi.sap.analysis

import com.hhi.sap.main.SparkSessionTestWrapper
import com.hhi.sap.table.term.TERM_MASTER
import com.hhi.sap.utils.SparkFileReader
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class ANL_THD_WEIGHT_WEEKTest extends FlatSpec with SparkSessionTestWrapper{
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val INPUTPATH = "src/test/resources"
  private val TABLE4 = "/output/table4"
  private val FILENPATH_ZPSCT600 = "/ZPDCV6021/*"
  private val FILENPATH_ZPDCT6023 = "/ZPDCT6023/*"
  private val FILENPATH_MARA = "/MARA/*"

  "ZPDCT6023 and MARA" should "be counted." in new SparkFileReader {
    println(getFolder(INPUTPATH+FILENPATH_ZPDCT6023).count())
    println(getFolder(INPUTPATH+FILENPATH_MARA).count())
  }

  "ZPDCT6023" should "make dataframe." in new SparkFileReader {
    //"Please be generated a table ZPDCT6023 on class ANL_THD_ZPSCT600_RTest "
    val tb_ZPDCT6023 = ss.read.option("header", "true").csv(INPUTPATH + TABLE4)
    val tb_MARA = ss.read.option("header", "true").csv(INPUTPATH + FILENPATH_MARA)

    new ANL_THD_WEIGHT_WEEK(ss.sqlContext).run(tb_ZPDCT6023
      .select(TERM_MASTER.ZPDCT6023.COMPANYID, TERM_MASTER.ZPDCT6023.SAUPBU, TERM_MASTER.ZPDCT6023.PSPID, TERM_MASTER.ZPDCT6023.STG_GUBUN, TERM_MASTER.ZPDCT6023.MAT_GUBUN, TERM_MASTER.ZPDCT6023.BRGEW, TERM_MASTER.ZPDCT6023.IDNRK, TERM_MASTER.ZPDCT6023.MENGE))
      .show()
  }
}