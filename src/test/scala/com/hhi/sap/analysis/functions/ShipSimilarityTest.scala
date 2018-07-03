package com.hhi.sap.analysis.functions

import com.hhi.sap.main.SparkSessionTestWrapper
import com.hhi.sap.utils.FileUtils
import org.scalatest.FlatSpec

class ShipSimilarityTest extends FlatSpec with SparkSessionTestWrapper{

  trait readFile {
    val fileName = "/EBAN/20180528094236_EBAN_2620.csv"
    val csv = ss.read.format("csv").option("header", "true").option("encoding", "EUC-KR").load(FileUtils().files(fileName).getPath)
  }

  "csv file" should "not null" in new readFile {
    assert(csv != null)
  }


  it should "show file contents" in new readFile {
    csv.show
  }
}
