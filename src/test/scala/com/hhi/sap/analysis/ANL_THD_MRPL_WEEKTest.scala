package com.hhi.sap.analysis

import com.hhi.sap.main.SparkSessionTestWrapper
import com.hhi.sap.table.term.TERM_MASTER
import com.hhi.sap.utils.SparkFileReader
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class ANL_THD_MRPL_WEEKTest extends FlatSpec with SparkSessionTestWrapper{
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val INPUTPATH = "src/test/resources"
  private val TABLE3 = "/output/table3"
  private val FILENPATH_ZPSCT600 = "/ZPDCV6021/*"
  private val FILENPATH_ZPDCT6123 = "/ZPDCT6123/*"

  "ZPDCT6123" should "be counted." in new SparkFileReader {
    println(getFolder(INPUTPATH+FILENPATH_ZPDCT6123).count())
  }

  "ZPDCT6123 " should "make dataframe." in new SparkFileReader {
    //"Please be generated a table ZPDCT6123 on class ANL_THD_ZPSCT600_RTest "
    val tb_ZPDCT6123 = ss.read.option("header", "true").csv(INPUTPATH + TABLE3)

    new ANL_THD_MRPL_WEEK(ss.sqlContext).run(tb_ZPDCT6123
      .select(TERM_MASTER.ZPDCT6123.COMPANYID, TERM_MASTER.ZPDCT6123.SAUPBU, TERM_MASTER.ZPDCT6123.PSPID, TERM_MASTER.ZPDCT6123.STG_GUBUN, TERM_MASTER.ZPDCT6123.MAT_GUBUN, TERM_MASTER.ZPDCT6123.WEEK))
      .show()
  }
}