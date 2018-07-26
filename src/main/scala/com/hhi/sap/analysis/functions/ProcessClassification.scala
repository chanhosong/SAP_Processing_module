package com.hhi.sap.analysis.functions

object ProcessClassification {
  private val MATCH_FAIL = "NOT_MATCHED"
  private val WRONG_PARSE = "WRONG PARSE"

  def getSTG_GUBUN(zmidactno: String, zhdrmatnr: String): String = {

    if(Option(zmidactno).isDefined && Option(zmidactno).isDefined){
      val (zzwkstg, zzwktyp) = (zmidactno.substring(11), zmidactno.substring(12, 13))

      zzwkstg match {
        case "A" | "B" => "1"
        case "C" => if (Option(zzwktyp).isEmpty) null else if (zzwktyp.contains("21")) "2" else "1"
        case "F" => "3"
        case "G" | "H" | "J"  => "5"
        case "K" | "L" | "M" => "6"
        case "P" => "7"
        case "R" => "8"
        case x =>
          if (Option(x).isEmpty) "NULL"
          else if (Option(zhdrmatnr).isEmpty) "NULL"
          else zhdrmatnr match {
            case x:String =>
              if(Option(x).isEmpty) "NULL"
              else if (x.matches(matchShipID)) "4"
              else "9"
          }
      }
    } else {
      MATCH_FAIL
    }
  }

  def getMAT_GUBUN(idnrk: String, lgort: String, paintgbn: String, zzmgroup: String): String = {
    val REGEX_PAINT = "[P|T|D|G|Q|R]"
    val REGEX_PAINT_NOT = "[^P|T|D|G|Q|R]"

    if(Option(idnrk).isDefined) {
      idnrk.substring(6, 8) match {
        case "PP" => "A"
        case _ => lgort match {
          case "PC50" =>
            if (Option(paintgbn).isEmpty) "NULL"
            else if (paintgbn.matches(REGEX_PAINT)) "B"
            else if (paintgbn.matches(REGEX_PAINT_NOT)) "C"
            else null
          case x: String =>
            if (Option(x).isEmpty) "NULL"
            else zzmgroup match {
              case "JY" => "D"
              case "JN" => "E"
              case "JU" => "F"
              case "JT" => "G"
              case "JC" => "H"
              case "JL" => "I"
              case "JW" => "J"
              case "JD" => "K"
              case "CV" => "M"
              case "CA" => "N"
              case "CB" => "O"
              case "CF" | "CG" => "P"
              case "CO" | "CR" => "Q"
              case "CL" | "CM" => "R"
              case "CP" => "S"
              case "CQ" => "T"
              case x: String =>
                if (Option(x).isEmpty) "NULL"
                else if (x.startsWith("J")) "L"
                else MATCH_FAIL
            }
        }
      }
    } else {
      MATCH_FAIL
    }
  }

  private val matchShipID = {
    val SHIPID_PRE = "H"
    val SHIPID_1 = "411E1"
    val SHIPID_2 = "423E11"
    val SHIPID_3 = "44097"
    val SHIPID_4 = "44099"
    val SHIPID_5 = "440E10"
    val SHIPID_6 = "440E12"
    val SHIPID_7 = "440E13"
    val SHIPID_8 = "440E14"
    val SHIPID_9 = "442E1"
    val SHIPID_10 = "443E1"
    val SHIPID_11 = "48+E1"

    s"[$SHIPID_PRE.*$SHIPID_1" +
      s"|$SHIPID_PRE.*$SHIPID_2" +
      s"|$SHIPID_PRE.*$SHIPID_3" +
      s"|$SHIPID_PRE.*$SHIPID_4" +
      s"|$SHIPID_PRE.*$SHIPID_5" +
      s"|$SHIPID_PRE.*$SHIPID_6" +
      s"|$SHIPID_PRE.*$SHIPID_7" +
      s"|$SHIPID_PRE.*$SHIPID_8" +
      s"|$SHIPID_PRE.*$SHIPID_9" +
      s"|$SHIPID_PRE.*$SHIPID_10" +
      s"|$SHIPID_PRE.*$SHIPID_11]"
  }
}
