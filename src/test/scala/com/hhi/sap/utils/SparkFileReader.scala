package com.hhi.sap.utils

import com.hhi.sap.main.SparkSessionTestWrapper
import org.apache.spark.sql.DataFrame

trait SparkFileReader extends SparkSessionTestWrapper{
  def getFile(fileName: String): DataFrame ={
    ss.read.format("csv").option("header", "true").option("encoding", "EUC-KR").load(FileUtils().files(fileName).getPath)
  }

  def getFolder(folderName: String): DataFrame ={
    ss.read.format("csv").option("header", "true").option("encoding", "EUC-KR").load(folderName)
  }
}