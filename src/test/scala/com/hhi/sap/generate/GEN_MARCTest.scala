package com.hhi.sap.generate

import com.hhi.sap.utils.SparkFileReader
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class GEN_MARCTest extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val INPUTPATH = "src/test/resources"
  private val OUTPUTPATH = "src/test/resources/output"
  private val TABLE6 = "/table6"
  private val FILENPATH_MARC = "/MARC/*"

  "MARC" should "be counted." in new SparkFileReader {
    println(getFolder(INPUTPATH+FILENPATH_MARC).count())
  }

  "MARC" should "be created csv file." in new SparkFileReader {
    val tb_MBEW = ss.read.option("header", "true").csv(INPUTPATH + FILENPATH_MARC)
    new GEN_MARC(ss.sqlContext).run(tb_MBEW).coalesce(1).write.option("header", "true").csv(OUTPUTPATH+TABLE6)
  }
}
