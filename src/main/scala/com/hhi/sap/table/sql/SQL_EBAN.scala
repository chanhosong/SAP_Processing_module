package com.hhi.sap.table.sql

import com.hhi.sap.table.term.TERM_MASTER

class SQL_EBAN {
  private val EBAN = TERM_MASTER.EBAN.TABLE

  val SQL_ALLDATA_EBAN = s"SELECT * FROM $EBAN"
}
