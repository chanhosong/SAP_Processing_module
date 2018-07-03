package com.hhi.sap.table.sql

import com.hhi.sap.table.term.TERM_MASTER

class SQL_MARC {
  private val MARC = TERM_MASTER.MARC.TABLE

  val SQL_ALLDATA_MARC = s"SELECT * FROM $MARC"
}
