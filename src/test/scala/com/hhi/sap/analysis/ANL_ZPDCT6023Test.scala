package com.hhi.sap.analysis

import com.hhi.sap.main.SparkSessionTestWrapper
import com.hhi.sap.table.term.{TERM_EBAN, TERM_MARA, TERM_ZPDCT6023}
import com.hhi.sap.utils.SparkFileReader
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class ANL_ZPDCT6023Test extends FlatSpec with SparkSessionTestWrapper{
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val OUTPUTPATH = "src/test/resources"
  private val TABLE3 = "table3"
  private val FILENPATH_ZPDCT6023 = "/ZPDCT6023/*"
  private val FILEPATH_EBAN = "/EBAN/*"
  private val FILEPATH_MARA = "/MARA/*"

  "ZPDCT6023" should "be counted." in new SparkFileReader {
    println(getFolder(OUTPUTPATH+FILENPATH_ZPDCT6023).count())
    println(getFolder(OUTPUTPATH+FILEPATH_EBAN).count())
    println(getFolder(OUTPUTPATH+FILEPATH_MARA).count())
  }

  "ZPDCT6023 " should "make dataframe." in new SparkFileReader {
    val ZPDCT6023 = getFolder(OUTPUTPATH+FILENPATH_ZPDCT6023).withColumnRenamed("bnfpo".toUpperCase(), TERM_EBAN.BFNPO)
    val eban = getFolder(OUTPUTPATH+FILEPATH_EBAN).withColumnRenamed("bnfpo".toUpperCase(), TERM_EBAN.BFNPO)
    val mara = getFolder(OUTPUTPATH+FILEPATH_MARA)

    new ANL_ZPDCT6023(ss.sqlContext).run(ZPDCT6023, eban.select(TERM_EBAN.BANFN, TERM_EBAN.BFNPO, TERM_EBAN.LGORT, TERM_EBAN.PAINTGBN), mara.select(TERM_MARA.MATNR, TERM_MARA.ZZMGROUP)).show(100)
  }
}
