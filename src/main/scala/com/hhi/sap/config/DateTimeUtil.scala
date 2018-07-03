package com.hhi.sap.config

import java.text.SimpleDateFormat
import java.util.Calendar

class DateTimeUtil {
  val date = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime)
  val time = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime)
  val weekNumber = Calendar.getInstance().get(Calendar.WEEK_OF_YEAR).toString
  val monthNumber = Calendar.getInstance().get(Calendar.MONTH).toString
}