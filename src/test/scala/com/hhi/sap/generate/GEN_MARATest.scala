package com.hhi.sap.generate

import com.hhi.sap.table.term.TERM_MASTER
import com.hhi.sap.utils.SparkFileReader
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class GEN_MARATest extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val INPUTPATH = "src/test/resources"
  private val OUTPUTPATH = "src/test/resources/output"
  private val TABLE5 = "/table5"
  private val FILENPATH_MARA = "/MARA/*"

  "MARA" should "be counted." in new SparkFileReader {
    println(getFolder(INPUTPATH+FILENPATH_MARA).count())
  }

  "MARA" should "be created csv file." in new SparkFileReader {
    val tb_MARA = ss.read.option("header", "true").csv(INPUTPATH + FILENPATH_MARA).withColumnRenamed("ZZDGOX", TERM_MASTER.MARA.ZZDGIOX)
    new GEN_MARA(ss.sqlContext).run(tb_MARA).coalesce(1).write.option("header", "true").csv(OUTPUTPATH+TABLE5)
  }
}
