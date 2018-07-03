package com.hhi.sap.table.sql

import com.hhi.sap.table.term.TERM_MASTER

class SQL_QBEW {
  private val QBEW = TERM_MASTER.QBEW.TABLE

  val SQL_ALLDATA_QBEW = s"SELECT * FROM $QBEW"
}
