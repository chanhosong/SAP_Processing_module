package com.hhi.sap.analysis.functions

import com.hhi.sap.table.term.TERM_MASTER
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

object ShipSimilarity_INLOCAL{
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val PT = 100.0

  private val FACTOR_RATE = "factor_rate".toUpperCase
  private val MATCH_FAIL_RATE = "match_fail_rate".toUpperCase
  private val SPECIAL_SHIP = "cn".toUpperCase

  def getSimilarityFromFile(factorMaster: Array[Row], ds1: Row, ds2: Row): Double = factorMaster.map(e=>compareCases(e, ds1, ds2)).sum

  private def compareCases(factor: Row, ds1: Row, ds2: Row): Double = {
    factor.getAs(TERM_MASTER.FACTOR.FACTOR_SEQ.toUpperCase).toString.trim match {
      case "01" => compareEqualsShip(TERM_MASTER.ZPSCT600.SHIP_KIND.toUpperCase, ds1, ds2, factor)
      case "02" => compareEqualsShip(TERM_MASTER.ZPSCT600.SHIP_TYPE_1.toUpperCase, ds1, ds2, factor)
      case "03" => compareEqualsShip(TERM_MASTER.ZPSCT600.DOCK.toUpperCase, ds1, ds2, factor)
      case "04" => compareEqualsShip(TERM_MASTER.ZPSCT600.BTYPE.toUpperCase, ds1, ds2, factor)
      case "05" => compareDuration(TERM_MASTER.ZPSCT600.DUR_AND.toUpperCase, ds1, ds2, factor)
      case "06" => compareDuration(TERM_MASTER.ZPSCT600.D1_ND.toUpperCase, ds1, ds2, factor)
      case "07" => compareDuration(TERM_MASTER.ZPSCT600.D2_ND.toUpperCase, ds1, ds2, factor)
      case "08" => compareDuration(TERM_MASTER.ZPSCT600.D3_ND.toUpperCase, ds1, ds2, factor)
      case "09" => compareDuration(TERM_MASTER.ZPSCT600.DUR_QND.toUpperCase, ds1, ds2, factor)
      case "10" => compareEqualsShipSpec(TERM_MASTER.ZPSCT600.SHIP_KIND.toUpperCase, ds1, ds2, factor)
      case "11" => compareWeight(TERM_MASTER.ZPSCT600.WEIGT_PR.toUpperCase, ds1, ds2, factor)
      case "12" => compareWeight(TERM_MASTER.ZPSCT600.WEIGT_BB.toUpperCase, ds1, ds2, factor)
      case "13" => compareWeight(TERM_MASTER.ZPSCT600.WEIGT_LD.toUpperCase, ds1, ds2, factor)
    }
  }

  private def compareEqualsShip(cps: String, c1: Row, c2: Row, factor: Row): Double = {
    if (c1.getAs(cps).toString == c2.getAs(cps).toString){
      Integer.parseInt(factor.getAs(FACTOR_RATE))
    } else {
      getFactor(factor.getAs(FACTOR_RATE), factor.getAs(MATCH_FAIL_RATE) , PT)
    }
  }

  private def compareEqualsShipSpec(cps: String, r1: Row, r2: Row, factor: Row): Double = {
    val shipKind = r1.getAs(cps).toString

    if (shipKind == r2.getAs(cps)){
      shipKind.toString match {
        case SPECIAL_SHIP => compareVolume(TERM_MASTER.ZPSCT600.CNTR.toUpperCase , r1, r2, factor)
        case _ => compareVolume(TERM_MASTER.ZPSCT600.DWT_SC.toUpperCase, r1, r2, factor)
      }
    } else {
      getFactor(factor.getAs(FACTOR_RATE), factor.getAs(MATCH_FAIL_RATE) , PT)
    }
  }

  private def compareDuration(cps: String, r1: Row, r2: Row, factor: Row): Double = {
    ((r1.getAs(cps): String).trim.toInt - (r2.getAs(cps): String).trim.toInt).abs match {
      case x if 0 until 10 contains x => getFactor(factor.getAs(FACTOR_RATE), factor.getAs(TERM_MASTER.FACTOR.MATCH01.toUpperCase), PT)
      case x if 11 until 30 contains x => getFactor(factor.getAs(FACTOR_RATE), factor.getAs(TERM_MASTER.FACTOR.MATCH02.toUpperCase), PT)
      case x if 31 until 50 contains x => getFactor(factor.getAs(FACTOR_RATE), factor.getAs(TERM_MASTER.FACTOR.MATCH03.toUpperCase), PT)
      case _ => getFactor(factor.getAs(FACTOR_RATE), factor.getAs(TERM_MASTER.FACTOR.MATCH04.toUpperCase), PT)
    }
  }

  private def compareWeight(cps: String, r1: Row, r2: Row, factor: Row): Double = {
    (r1.getAs(cps).toString.toDouble - r2.getAs(cps).toString.toDouble).abs match {
      case x if 0 until 1000 contains x => getFactor(factor.getAs(FACTOR_RATE), factor.getAs(TERM_MASTER.FACTOR.MATCH01.toUpperCase), PT)
      case x if 1001 until 3000 contains x => getFactor(factor.getAs(FACTOR_RATE), factor.getAs(TERM_MASTER.FACTOR.MATCH02.toUpperCase), PT)
      case x if 3001 until 5000 contains x => getFactor(factor.getAs(FACTOR_RATE), factor.getAs(TERM_MASTER.FACTOR.MATCH03.toUpperCase), PT)
      case _ => getFactor(factor.getAs(FACTOR_RATE), factor.getAs(TERM_MASTER.FACTOR.MATCH04.toUpperCase), PT)
    }
  }

  private def compareVolume(cps: String, r1: Row, r2: Row, factor: Row): Double = {
    (r1.getAs(cps).toString.toDouble - r2.getAs(cps).toString.toDouble).abs match {
      case x if 0 until 500 contains x => getFactor(factor.getAs(FACTOR_RATE), factor.getAs(TERM_MASTER.FACTOR.MATCH01.toUpperCase), PT)
      case x if 501 until 1000 contains x => getFactor(factor.getAs(FACTOR_RATE), factor.getAs(TERM_MASTER.FACTOR.MATCH02.toUpperCase), PT)
      case _ => getFactor(factor.getAs(FACTOR_RATE), factor.getAs(TERM_MASTER.FACTOR.MATCH04.toUpperCase), PT)
    }
  }

  private def getFactor(f1: String, f2: String, f3: Double): Double = Integer.parseInt(f1) * Integer.parseInt(f2) / f3
}