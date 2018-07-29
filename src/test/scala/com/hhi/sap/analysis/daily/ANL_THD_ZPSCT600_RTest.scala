package com.hhi.sap.analysis.daily

import com.hhi.sap.main.SparkSessionTestWrapper
import com.hhi.sap.table.term.TERM_MASTER
import com.hhi.sap.utils.SparkFileReader
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class ANL_THD_ZPSCT600_RTest extends FlatSpec with SparkSessionTestWrapper{
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val OUTPUTPATH = "src/test/resources/output/"
  private val TABLE1 = "table1"
  private val FILENAME = "/ZPDCV6021/20180619070731_ZPDCV6021.csv"

  "ZPDCV6021" should "be counted." in new SparkFileReader {
    println(getFile(FILENAME).rdd.count())
  }

  "Similarity method" should "make dataframe." in new SparkFileReader {
    val df = new ANL_THD_ZPSCT600_R_INLOCAL(ss.sqlContext).runFromFile(getFile(FILENAME))
    df.coalesce(1).write.option("header", "true").csv(OUTPUTPATH+TABLE1)
    df.show()
  }

  "SERNO" should "be sequential." in new SparkFileReader {
    val df = ss.read.option("header", "true").csv(OUTPUTPATH+TABLE1).where(s"${TERM_MASTER.ZPSCT600_R.SERNO} == 1 OR ${TERM_MASTER.ZPSCT600_R.SERNO} == 2")
    df.show()
  }

  "RANKING" should "be ranked." in new SparkFileReader {
    val df = ss.read.option("header", "true").csv(OUTPUTPATH+TABLE1).where(s"${TERM_MASTER.ZPSCT600_R.RANKING} == 1").select(TERM_MASTER.ZPSCT600_R.PSPID, TERM_MASTER.ZPSCT600_R.RANKING).distinct()
    df.show()
  }
}