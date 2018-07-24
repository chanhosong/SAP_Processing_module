package com.hhi.sap.generate

import com.hhi.sap.table.term.TERM_MASTER
import com.hhi.sap.utils.SparkFileReader
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class GEN_QBEWTest extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val INPUTPATH = "src/test/resources"
  private val OUTPUTPATH = "src/test/resources/output"
  private val TABLE8 = "/table8"
  private val FILENPATH_QBEW = "/QBEW/*"

  "QBEW" should "be counted." in new SparkFileReader {
    println(getFolder(INPUTPATH+FILENPATH_QBEW).count())
  }

  "QBEW" should "be created csv file." in new SparkFileReader {
    val tb_QBEW = ss.read.option("header", "true").csv(INPUTPATH + FILENPATH_QBEW).withColumnRenamed("BWKEY", TERM_MASTER.QBEW.WERKS).withColumnRenamed("PSPNR", TERM_MASTER.QBEW.PSPID)
    new GEN_QBEW(ss.sqlContext).run(tb_QBEW).coalesce(1).write.option("header", "true").csv(OUTPUTPATH+TABLE8)
  }
}
