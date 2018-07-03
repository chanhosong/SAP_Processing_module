package com.hhi.sap.config

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

class DateTimeUtil {
  private val dateFormat = "yyyy-MM-dd"
  private val timeFormat = "HH:mm:ss"
  private val dateSDF = new SimpleDateFormat(dateFormat)
  private val timeSDF = new SimpleDateFormat(timeFormat)

  val date = dateSDF.format(Calendar.getInstance().getTime)
  val time = timeSDF.format(Calendar.getInstance().getTime)

  def getWeekDifference(zexdate: String, wc: String): Int = weekNumber(dateSDF.parse(zexdate)) -  weekNumber(dateSDF.parse(wc))
  def getMonthDifference(zexdate: String, wc: String): Int = monthNumber(dateSDF.parse(zexdate)) -  monthNumber(dateSDF.parse(wc))

  private def weekNumber(yyyyMMdd: Date) = {
    val calendar = Calendar.getInstance()
    calendar.setTime(yyyyMMdd)
    calendar.get(Calendar.WEEK_OF_YEAR)
  }

  private def monthNumber(yyyyMMdd: Date) = {
    val calendar = Calendar.getInstance()
    calendar.setTime(yyyyMMdd)
    calendar.get(Calendar.MONTH)
  }
}