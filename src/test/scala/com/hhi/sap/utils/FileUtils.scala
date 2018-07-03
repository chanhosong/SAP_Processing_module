package com.hhi.sap.utils

import java.net.URL

case class FileUtils() {
  def files(fileName: String): URL = {
    getClass.getResource(fileName)
  }

  def getLinesFromStream(fileName: String): Iterator[String] = {
    val fileStream = getClass.getResourceAsStream(fileName)
    scala.io.Source.fromInputStream(fileStream).getLines
  }
}
