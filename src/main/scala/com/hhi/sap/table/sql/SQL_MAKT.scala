package com.hhi.sap.table.sql

import com.hhi.sap.table.term.TERM_MASTER

class SQL_MAKT {
  private val MAKT = TERM_MASTER.MAKT.TABLE

  val SQL_ALLDATA_MAKT = s"SELECT * FROM $MAKT"
}
