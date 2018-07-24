package com.hhi.sap.generate

import com.hhi.sap.table.term.TERM_MASTER
import com.hhi.sap.utils.SparkFileReader
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class GEN_MBEWTest extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val INPUTPATH = "src/test/resources"
  private val OUTPUTPATH = "src/test/resources/output"
  private val TABLE9 = "/table9"
  private val FILENPATH_MBEW = "/MBEW/*"

  "MBEW" should "be counted." in new SparkFileReader {
    println(getFolder(INPUTPATH+FILENPATH_MBEW).count())
  }

  "MBEW" should "be created csv file." in new SparkFileReader {
    val tb_MBEW = ss.read.option("header", "true").csv(INPUTPATH + FILENPATH_MBEW).withColumnRenamed("BWKEY", TERM_MASTER.MBEW.WERKS)
    new GEN_MBEW(ss.sqlContext).run(tb_MBEW).coalesce(1).write.option("header", "true").csv(OUTPUTPATH+TABLE9)
  }
}
