package com.hhi.sap.analysis.functions

object ProcessClassification {
  private val MATCH_FAIL = "NOT_MATCHED"
  private val WRONG_PARSE = "WRONG PARSE"

  def getSTG_GUBUN(zmidactno: String, zhdrmatnr: String): String = {
    val (zzwkstg, zzwktyp) = (zmidactno.substring(11), zmidactno.substring(12, 13))

    zzwkstg match {
      case "A" | "B" => "조립"
      case "C" => if (Option(zzwktyp).isEmpty) null else if (zzwktyp.contains("21")) "INSHOP" else "조립"
      case "F" => "선행의장"
      case "G" | "H" | "J"  => "PE의장"
      case "K" | "L" | "M" => "후PE의장"
      case "P" => "후행의장"
      case "R" => "시운전"
      case x =>
        if (Option(x).isEmpty) null
        else if (Option(zhdrmatnr).isEmpty) null
        else zhdrmatnr match {
        case x:String =>
          if(Option(x).isEmpty) null
          else if (x.matches(matchShipID)) "UNIT"
          else "기타"
      }
    }
  }

  def getMAT_GUBUN(idnrk: String, lgort: String, paintgbn: String, zzmgroup: String): String = {
    val REGEX_PAINT = "[P|T|D|G|Q|R]"
    val REGEX_PAINT_NOT = "[^P|T|D|G|Q|R]"

    idnrk.substring(6,8) match {
      case "PP" => "PIPE_PS'S"
      case _ => lgort match {
        case "PC50" =>
          if (Option(paintgbn).isEmpty) null
          else if (paintgbn.matches(REGEX_PAINT)) "냉천도장"
          else if (paintgbn.matches(REGEX_PAINT_NOT)) "야드직투입"
          else null
        case x:String =>
          if (Option(x).isEmpty) null
          else zzmgroup match {
            case "JY" => "기장"
            case "JN" => "선실"
            case "JU" => "선장"
            case "JT" => "전장"
            case "JC" => "케이블"
            case "JL" => "LIGHTING"
            case "JW" => "보온재"
            case "JD" => "PUMP"
            case "CV" => "VALVE"
            case "CA" => "GASKET"
            case "CB" => "BOLT/NUT"
            case "CF" | "CG" => "FLANGE"
            case "CO" | "CR" => "STEEL_FITTING"
            case "CL" | "CM" => "비철_FITTING"
            case "CP" => "STEEL_PIPE"
            case "CQ" => "비철_PIPE"
            case x:String =>
              if (Option(x).isEmpty) null
              else if (x.startsWith("J")) "기타"
              else MATCH_FAIL
        }
      }
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
