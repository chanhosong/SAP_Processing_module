package com.hhi.sap.table.sql

import com.hhi.sap.table.term.TERM_MASTER

class SQL_MBEW {
  private val MBEW = TERM_MASTER.MBEW.TABLE

  val SQL_ALLDATA_MBEW = s"SELECT * FROM $MBEW"
}
