package com.hhi.sap.table.factor

import java.net.URL

import com.hhi.sap.main.SparkSessionWrapper
import com.hhi.sap.table.sql.SQL_MASTER
import org.apache.spark.sql.DataFrame

object FactorMasterTableFromLocal extends SparkSessionWrapper{
  val FILENAME = "/FACTOR/factor_master.csv"
  def getTable: DataFrame = ss.read.format("csv").option("header", "true").option("encoding", "EUC-KR").load(FileUtils().files(FILENAME).getPath).where(SQL_MASTER.FACTOR.SQL_INQUIRE_FACTOR)

  case class FileUtils() {
    def files(fileName: String): URL = {
      getClass.getResource(fileName)
    }

    def getLinesFromStream(fileName: String): Iterator[String] = {
      val fileStream = getClass.getResourceAsStream(fileName)
      scala.io.Source.fromInputStream(fileStream).getLines
    }
  }
}
