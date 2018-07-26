package com.hhi.sap.generate

import com.hhi.sap.utils.SparkFileReader
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class GEN_MAKTTest extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val INPUTPATH = "src/test/resources"
  private val OUTPUTPATH = "src/test/resources/output"
  private val TABLE7 = "/table7"
  private val FILENPATH_MAKT = "/MAKT/*"

  "MAKT" should "be counted." in new SparkFileReader {
    println(getFolder(INPUTPATH+FILENPATH_MAKT).count())
  }

  "MAKT" should "be created csv file." in new SparkFileReader {
    val tb_MAKT = ss.read.option("header", "true").csv(INPUTPATH + FILENPATH_MAKT)
    new GEN_MAKT(ss.sqlContext).run(tb_MAKT).coalesce(1).write.option("header", "true").csv(OUTPUTPATH+TABLE7)
  }
}
