package com.hhi.sap.main

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

trait SparkSessionTestWrapper {
  private val SPARK_MASTER = "local[*]"
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  lazy val ss: SparkSession = getSparkSession

  private  def getSparkSession: SparkSession = {
    if (logger.isWarnEnabled) logger.debug(s"Spark Master is $SPARK_MASTER")
    SparkSession.builder().appName("SAP Analytics App").master(SPARK_MASTER).getOrCreate()
  }
}