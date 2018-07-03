package com.hhi.sap.config

import scala.io.Source.fromURL

class PropertyLoader {
  private val FILENAME = "application.properties"

  private val reader = fromURL(getClass.getResource(FILENAME)).bufferedReader()

}
