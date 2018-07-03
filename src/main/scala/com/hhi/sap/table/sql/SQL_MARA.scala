package com.hhi.sap.table.sql

import com.hhi.sap.table.term.TERM_MASTER

class SQL_MARA {
  private val MARA = TERM_MASTER.MARA.TABLE

  val SQL_ALLDATA_MARA = s"SELECT * FROM $MARA"
}
