package com.hhi.sap.analysis

import com.hhi.sap.main.SparkSessionTestWrapper
import com.hhi.sap.table.term.TERM_MASTER
import com.hhi.sap.utils.SparkFileReader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class ANL_THD_ZPSCT600_RTest extends FlatSpec with SparkSessionTestWrapper{
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val OUTPUTPATH = "src/test/resources/output/"
  private val TABLE1 = "table1"
  private val TABLE2 = "table2"
  private val FILENAME = "/ZPDCV6021/20180619070731_ZPDCV6021.csv"

  "ZPDCV6021" should "be counted." in new SparkFileReader {
    println(getFile(FILENAME).rdd.count())
  }

  "Similarity method" should "make dataframe." in new SparkFileReader {
    new ANL_THD_ZPSCT600_R_INLOCAL(ss.sqlContext).runFromFile(getFile(FILENAME))
      .limit(2000).coalesce(1).write.option("header", "true").csv(OUTPUTPATH+TABLE1)
  }

  "Similarity csv file" should "be correctly matched the SERNO and RANKING." in new SparkFileReader {
    ss.read.option("header", "true").csv(OUTPUTPATH+TABLE1)
      .withColumn(TERM_MASTER.ZPSCT600_R.SERNO, row_number().over(Window.partitionBy(TERM_MASTER.ZPSCT600_R.PSPID).orderBy(TERM_MASTER.ZPSCT600_R.PSPID_A)))
      .withColumn(TERM_MASTER.ZPSCT600_R.RANKING, rank().over(Window.partitionBy(TERM_MASTER.ZPSCT600_R.PSPID).orderBy(TERM_MASTER.ZPSCT600_R.RANK_RATE)))
      .coalesce(1)
      .write.option("header", "true").csv(OUTPUTPATH+TABLE2)
  }

  "SERNO" should "be sequential." in new SparkFileReader {
    val df = ss.read.option("header", "true").csv(OUTPUTPATH+TABLE2).where("serno == 1 OR serno == 2")
    df.show()
    assert(df.count == 4)
  }

  "RANKING" should "be ranked." in new SparkFileReader {
    val df = ss.read.option("header", "true").csv(OUTPUTPATH+TABLE2).where("ranking == 1").select("PSPID","ranking").distinct()
    df.show()
    assert(df.count == 2)
  }
}