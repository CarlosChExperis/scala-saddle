package com.breeze

import java.time.Instant
import com.breeze.StackUtils.{stack, stackSeries, stackVecBool}
import java.time.Duration
import org.saddle.io.CsvImplicits._
import org.saddle.{Frame, Index, Panel, Series, Vec}
import org.saddle.index.{InnerJoin, LeftJoin, OuterJoin}
import org.saddle.io.{CsvFile, CsvParser}

import scala.io.Source

object DevelopeStack {

  def main(args: Array[String]): Unit = {

//    val start2 = Instant.now()
//    val forecast = CsvFile("C:\\data\\forecast - copia.csv")
    val forecast = CsvFile("C:\\data\\forecast.csv")
    val cells = CsvFile("C:\\data\\cells.csv")
    val bottlenecks = CsvFile("C:\\data\\bottlenecks.csv")

    val rule = Source.fromFile("C:\\data\\rule.txt").mkString
    val tech = Source.fromFile("C:\\data\\technology.txt").mkString.trim

    val template = readTemplate(tech)

    val frame_forecast = CsvParser.parse()(forecast).withRowIndex(0).withColIndex(0)
    val frame_cells = CsvParser.parse()(cells).withRowIndex(17).withColIndex(0)
      .rmask(Vec(true, true, false, false, false,
        false, false, false, true, true,
        true, true,true, true,true,
        true,true, true,true, true,
        true, true, true, false, true,
        true,true, true,true))
      .squeeze

    val frame_bottleneck = CsvParser.parse()(bottlenecks).withRowIndex(0).withColIndex(0)

    val start = Instant.now
    val frame_forecast_cells = frame_forecast.join(frame_cells, how = LeftJoin)

//    val aux = CsvFile("C:\\data\\forecast - copia.csv")
//    val aux2 = CsvParser.parse()(cells).withColIndex(0)

    val refactForecastCells = frame_forecast_cells.setColIndex(
        Index(
          "date",
          template.get("traffic_dl_kbps").head,
          template.get("traffic_ul_kbps").head,
          template.get("voice_erlang").head,
          template.get("DL_active_users").head,
          template.get("UL_active_users").head,
          template.get("RRC_connected_users").head,
          template.get("RSSI").head,
          template.get("DL_spec_eff").head,
          template.get("UL_spec_eff").head,
          template.get("DL_PRB_util").head,
          template.get("UL_PRB_util").head,
          template.get("DL_Sched_Ent").head,
          template.get("UL_Sched_Ent").head,
          template.get("CQI").head,
          template.get("DL_CET").head,
          template.get("UL_CET").head,
          template.get("DL_user_thp").head,
          template.get("UL_user_thp").head,

          template.get("hl").head,
          template.get("carrier").head,
          template.get("dl.carrierbw").head,
          template.get("ul.carrierbw").head,
          template.get("CFI").head,
          template.get("CFI.dynamic").head,
          template.get("market").head
        )
    )

//    var contadorId = 0;
//    val forecastCellsFrame = refactForecastCells.mapRowIndex(x=>{contadorId=contadorId+1;x+"."+contadorId.toString;})

//    frame_forecast_cells.setColIndex(Index(""))
    //CREO TRUE
    //    val lista_bool: List[Boolean] = List.fill(frame_bottleneck.numRows)(true)
//    val frame_bool = Panel(Vec(lista_bool:_*))

    val cleanRule = rule.replace("\r", "")
      .replace(",", ".")
      .replace("\n", "")
      .replace("(", " ( ")
      .replace(")", " ) ")
      .replace("*", " * ")
      .replace("AND", " AND ")
      .replace("OR", " OR ")
      .replace("IF", " IF ")
      .replace("ELSE", " ELSE ")
      .replace("<", " < ")
      .replace(">", " > ")
      .replace("> =", " >= ")
      .replace("< =", " <= ")
    //borrar EndIF y derivados

    val listRule = cleanRule.split(" ").filter(x=>x!="")

    val stackProcess = stack

    val listPossibleItems: List[String] = List("(", ")", ">", "AND", "OR", ">=", "*", "+", "-","/", "<")
    val listOperadoresArimeticos: List[String] = List("*", "+", "-","/")

    val listaSufija2 = infijaToSufija(listRule)

    val start2 = Instant.now
    for (j <- 0 until listaSufija2.length){
      val item = listaSufija2(j)
      println("item:" + item + " "+ j.toString)
      if (!listPossibleItems.contains(item)){
        stack.push(item)
      }
      else {
        if (item=="AND" | item== "OR") {
          val serie1:Vec[Boolean] = stackVecBool.top
          stackVecBool.pop
          val serie2:Vec[Boolean] = stackVecBool.top
          stackVecBool.pop

          if (item=="AND") {
            val resultadoSerie = serie1 && serie2
            stackVecBool.push(resultadoSerie)
          }
          else{
            val resultadoSerie = serie1 || serie2
            stackVecBool.push(resultadoSerie)
          }
        }
        else{
          var operando2: Double = 0
          var operando1: String = ""
          val firstItem = stackProcess.top
          val boolOperando_aux: Boolean = stackProcess.length > 1

          if (boolOperando_aux){
            if(firstItem.toString.replace(".","") forall Character.isDigit){
              operando2 = stackProcess.top.toDouble
              stackProcess.pop
              operando1 = stackProcess.top.toString
              stackProcess.pop
            }
            else{
              operando1 = stackProcess.top.toString
              stackProcess.pop
              operando2 = stackProcess.top.toDouble
              stackProcess.pop
            }
            if(listOperadoresArimeticos.contains(item)){
              val resultado: Series[String, String] = logicaSaddleAritmeticos(refactForecastCells, item, operando1, operando2)
              stackSeries.push(resultado)
            }
            else{
              val resultado: Vec[Boolean] = logicaSaddleLogicos(refactForecastCells, item, operando1, operando2)
//              Frame("x"->resultado).writeCsvFile("condicion1.csv")
              stackVecBool.push(resultado)
            }
          }
          else{
            val boolOperando1: Boolean = firstItem forall Character.isDigit
            val boolOperando3: Boolean = stackSeries.nonEmpty

            if (!boolOperando1 && boolOperando3){
              val topStackSerie = stackSeries.top
              stackSeries.pop
              val topStack = stackProcess.top
              stackProcess.pop
              val resultado = logicaSaddleSeries(refactForecastCells,item, topStack,topStackSerie)
              stackVecBool.push(resultado)
            }else{
              if(firstItem.toString.replace(".","") forall Character.isDigit){
                operando2 = stackProcess.top.toDouble
                stackProcess.pop
                operando1 = stackProcess.top.toString
                stackProcess.pop
              }
              else{
                operando1 = stackProcess.top.toString
                stackProcess.pop
                operando2 = stackProcess.top.toDouble
                stackProcess.pop
              }
              if(listOperadoresArimeticos.contains(item)){
                val resultado: Series[String, String] = logicaSaddleAritmeticos(refactForecastCells, item, operando1, operando2)
                stackSeries.push(resultado)
              }
              else{
                val resultado: Vec[Boolean] = logicaSaddleLogicos(refactForecastCells, item, operando1, operando2)
                stackVecBool.push(resultado)
              }
            }
          }

        }
      }
    }

    val resultado = stackVecBool.top
    stackVecBool.pop
    val finish2: Instant = Instant.now
    val timeElapsedReadDataCSV2: Long = Duration.between(start2, finish2).toMillis
    println("El proceso ha tardado: " + timeElapsedReadDataCSV2 / 1000.0)
    val getPreconditionBottleneck = getPrecondition(refactForecastCells)
    val resultadoFinal = resultado && getPreconditionBottleneck
    val indexCols = refactForecastCells.rowIx.toVec
    val dateCols = refactForecastCells.colAt(0).toVec

    Frame("index" -> indexCols, "date" -> dateCols, "resultado" -> resultadoFinal.map(x=>x.toString)).writeCsvFile("prueba.csv")

    val finish: Instant = Instant.now
    val timeElapsedReadDataCSV: Long = Duration.between(start, finish).toMillis
    println("El proceso ha tardado: " + timeElapsedReadDataCSV / 1000.0)
  }

  def logicaSaddleLogicos(df: Frame[String, String, String], operadorLogico: String, variable: String, numero: Double): Vec[Boolean] ={
    var resultado: Vec[Boolean] = Vec()

    if(operadorLogico==">") {
      val columnFrame = df.col(variable)
      resultado = columnFrame.colAt(0).toVec.map(x=>x.toDouble) > numero.toDouble
    }
    if(operadorLogico==">=") {
      val columnFrame = df.col(variable)
      resultado = columnFrame.colAt(0).toVec.map(x=>x.toDouble) >= numero.toDouble
    }
    if(operadorLogico=="<=") {
      val columnFrame = df.col(variable)
      resultado = columnFrame.colAt(0).toVec.map(x=>x.toDouble) <= numero.toDouble
    }
    if(operadorLogico=="<") {
      val columnFrame = df.col(variable)
      resultado = columnFrame.colAt(0).toVec.map(x=>x.toDouble) < numero.toDouble
    }
    if(operadorLogico=="==") {
      val columnFrame = df.col(variable)
      resultado = columnFrame.colAt(0).toVec.map(x=>x.toDouble) =? numero.toDouble
    }
    if(operadorLogico=="!=") {
      val columnFrame = df.col(variable)
      resultado = columnFrame.colAt(0).toVec.map(x=>x.toDouble) <> numero.toDouble
    }
    resultado
  }

  def logicaSaddleAritmeticos(df: Frame[String, String, String], operadorLogico: String, variable: String, numero: Double): Series[String, String] ={
    var resultado: Series[String, String] = Series()

    if(operadorLogico=="*") {
      val columnFrame = df.col(variable)
//      resultado = columnFrame.colAt(0).mapColIndex(x=>(x.toDouble * numero.toDouble).toString).colAt(0)
      resultado = columnFrame.colAt(0).mapValues{case t => (t.toDouble * numero.toDouble).toString}
    }
    resultado
  }

  def logicaSaddleSeries(df: Frame[String, String, String], operadorLogico: String, variable: String, serie: Series[String, String]): Vec[Boolean]={
    var resultado: Vec[Boolean] = Vec()

    if(operadorLogico==">") {
      val columnFrame = df.col(variable)
//      resultado = serie.toVec.values.map(x=>x.toDouble) > serie.toVec.values.map(x=>x.toDouble)
      resultado = columnFrame.colAt(0).toVec.values.map(x=>x.toDouble) > serie.toVec.values.map(x=>x.toDouble)
    }
    resultado
  }

  def getPrecondition(df: Frame[String, String, String]): Vec[Boolean] ={
    val getVoiceErlang = df.col("Voice").colAt(0).toVec
    val getTrafficDlKbps = df.col("TrafficDL").colAt(0).toVec
    val getTrafficUlKbps = df.col("TrafficUL").colAt(0).toVec

    val sum1 = getVoiceErlang.map(x=>x.toDouble) + getTrafficDlKbps.map(x=>x.toDouble)
    val sum2 = getVoiceErlang.map(x=>x.toDouble) + getTrafficUlKbps.map(x=>x.toDouble)

    val condition1 = sum1 <> 0
    val condition2 = sum2 <> 0

    condition1 && condition2
  }

  def readTemplate(tech: String) : Map[String, String] = {
    val pathTemplate = "C:/data/columns.csv"
    val bufferedSource = Source.fromFile(pathTemplate)
    var dictTemplate: Map[String,String] = Map()

//    var listTemplate: List[String] = List()
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      //filter template by technology
//      if (tech==cols.head) listTemplate = listTemplate :+ s"${cols(2)} as ${cols(3)}".replace(".", "_")
      if (tech==cols.head) dictTemplate = dictTemplate + (cols(2) -> cols(3))
    }
    dictTemplate
  }

  def infijaToSufija(listRule: Array[String]): List[String] = {
    val lenListRule = listRule.length
    var listaSufija: List[String] = List()
    val stackProcess = stack

    val weightRule = Map("(" -> 1, "-" -> 8, "+" -> 8, "/" ->9, "*" ->9 ,
      //      "<" -> 4, ">" -> 4, ">=" -> 4, "<=" -> 4,"AND" -> 5, "OR" -> 5, "IF" -> 6, "ELSE" -> 6,
      "<" -> 7, ">" -> 7, ">=" -> 7, "<=" -> 7,"AND" -> 5, "OR" -> 5, "IF" -> 4, "ELSE" -> 4,
      "CASE" -> 7, "VALUE" -> 7
    )

    val listPossibleItems: List[String] = List("(", ")", ">", "AND", "OR", ">=", "*", "+", "-","/", "<")

    for (i <- 0 until lenListRule){
      val itemRule = listRule(i).toString
      if (!listPossibleItems.contains(itemRule)) {
        listaSufija = listaSufija :+ itemRule
      }
      else {
        if (itemRule == "(") {
          stackProcess.push(itemRule)
        }
        else {
          if (itemRule == ")") {
            var symbolTop = stackProcess.top
            stackProcess.pop
            while (symbolTop != "(") {
              listaSufija = listaSufija :+ symbolTop
              symbolTop = stackProcess.top
              stackProcess.pop
            }
          }
          else{
            while (!stackProcess.isEmpty && (weightRule.get(stackProcess.top).head >= weightRule.get(itemRule).head)){
              val getItemStack = stackProcess.top
              listaSufija = listaSufija :+ getItemStack.toString
              stackProcess.pop
            }
            stackProcess.push(itemRule)
          }
        }
      }
    }
    while (!stackProcess.isEmpty){
      val getItemStack = stackProcess.top
      //      listaSufija = listaSufija :+ getItemStack.head.toString
      listaSufija = listaSufija :+ getItemStack.toString
      stackProcess.pop
    }
    listaSufija
  }

}

