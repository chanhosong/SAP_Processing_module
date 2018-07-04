package com.hhi.sap.analysis

import com.hhi.sap.main.SparkSessionTestWrapper
import com.hhi.sap.table.term.TERM_MASTER
import com.hhi.sap.utils.SparkFileReader
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class ANL_ZPDCT6023Test extends FlatSpec with SparkSessionTestWrapper{
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val OUTPUTPATH = "src/test/resources"
  private val TABLE3 = "table3"
  private val FILENPATH_ZPSCT600 = "/ZPDCV6021/*"
  private val FILENPATH_ZPDCT6023 = "/ZPDCT6023/*"
  private val FILEPATH_EBAN = "/EBAN/*"
  private val FILEPATH_MARA = "/MARA/*"

  "ZPDCT6023" should "be counted." in new SparkFileReader {
    println(getFolder(OUTPUTPATH+FILENPATH_ZPDCT6023).count())
    println(getFolder(OUTPUTPATH+FILEPATH_EBAN).count())
    println(getFolder(OUTPUTPATH+FILEPATH_MARA).count())
  }

  "ZPDCT6023 " should "make dataframe." in new SparkFileReader {
    val tb_ZPSCT600 = ss.read.option("header", "true").csv(OUTPUTPATH+FILENPATH_ZPSCT600)
    val ZPDCT6023 = getFolder(OUTPUTPATH+FILENPATH_ZPDCT6023).withColumnRenamed("bnfpo".toUpperCase(), TERM_MASTER.EBAN.BFNPO)
    val eban = getFolder(OUTPUTPATH+FILEPATH_EBAN).withColumnRenamed("bnfpo".toUpperCase(), TERM_MASTER.EBAN.BFNPO)
    val mara = getFolder(OUTPUTPATH+FILEPATH_MARA)

    new ANL_ZPDCT6023(ss.sqlContext).run(ZPDCT6023, tb_ZPSCT600
      , eban.select(TERM_MASTER.EBAN.COMPANYID, TERM_MASTER.EBAN.SAUPBU, TERM_MASTER.EBAN.BANFN, TERM_MASTER.EBAN.BFNPO, TERM_MASTER.EBAN.LGORT, TERM_MASTER.EBAN.PAINTGBN)
      , mara.select(TERM_MASTER.MARA.COMPANYID, TERM_MASTER.MARA.SAUPBU, TERM_MASTER.MARA.MATNR, TERM_MASTER.MARA.ZZMGROUP))
      .show(100)
  }
}
