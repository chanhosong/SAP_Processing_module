package com.hhi.sap.main

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

trait SparkSessionWrapper {
  private val logger: Logger = LoggerFactory.getLogger(SparkSessionWrapper.super.getClass.getName)
  lazy val ss: SparkSession = getSparkSession

  private  def getSparkSession: SparkSession = {
    if (logger.isInfoEnabled()) {
      SparkSession.builder().appName("SAP Analytics App").master("yarn").enableHiveSupport().getOrCreate()
    } else {
      SparkSession.builder().appName("SAP Analytics App").master("local[*]").getOrCreate()
    }
  }
}