package com.hhi.sap.table.sql

import com.hhi.sap.table.term.TERM_MASTER

class SQL_ZPDCV6021 {
  private val THD_ZPDCV6021 = TERM_MASTER.ZPDCV6021.TABLE

  val SQL_ALLDATA_THD_ZPDCV6021 = s"SELECT * FROM $THD_ZPDCV6021"
  val SQL_COSTAT_N = "CO_STAT == 'N'"
  val SQL_COSTAT_Y = "CO_STAT == 'Y'"
}