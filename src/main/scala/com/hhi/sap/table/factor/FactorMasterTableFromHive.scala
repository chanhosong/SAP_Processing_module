package com.hhi.sap.table.factor

import com.hhi.sap.main.SparkSessionWrapper
import com.hhi.sap.table.sql.SQL_MASTER
import org.apache.spark.sql.DataFrame

object FactorMasterTableFromHive extends SparkSessionWrapper{
  def getTable: DataFrame = ss.sqlContext.sql(SQL_MASTER.FACTOR.SQL_INQUIRE_FACTOR)
}

class FactorMasterTableFromHive extends SparkSessionWrapper{
  def getTable: DataFrame = ss.sqlContext.sql(SQL_MASTER.FACTOR.SQL_INQUIRE_FACTOR)
}