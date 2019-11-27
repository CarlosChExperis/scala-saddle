package com.breeze

import scala.io.Source
import java.time.Instant
import java.time.Duration

import org.saddle.io.{CsvFile, CsvParser}
import scala.util.control.Breaks.break


object Develop {
  def main(args: Array[String]): Unit = {
    val rule = Source.fromFile("C:\\data\\rule.txt").mkString

    val start = Instant.now

    //falta iterarlo n-veces
    val cleanRule = transformRule(rule)
    println(cleanRule)


    val finish: Instant = Instant.now
    val timeElapsedReadDataCSV: Long = Duration.between(start, finish).toMillis
    println("El proceso ha tardado: " + timeElapsedReadDataCSV / 1000.0)

//    val contador2 = 0
//    val longcleanRule = cleanRule.length
//    for (k <- 0 until longcleanRule){
//      if (cleanRule(k)=="(") contador2 = contador2 + 1
//    }
//    var forecast_array:Array[String] = Array()
//    val bufferedSourceForecast = io.Source.fromFile("C:\\data\\forecast.csv")
//    for (line <- bufferedSourceForecast.getLines) {
//      val col = line.split(",")(0)
//      forecast_array = forecast_array :+ col
//    }
//    bufferedSourceForecast.close
//
//    forecast_array = forecast_array.drop(1)

    val forecast = CsvFile("C:\\data\\forecast.csv")
    val frame_forecast = CsvParser.parse()(forecast).withColIndex(0)

//    val start = Instant.now

    //funcionaaaaaaaaaaaa
//    var aux :Array[Boolean]= Array()
//    for(i <- 0 until 39) {
//      //val aux = forecast_array.filter(x => x.toInt > 5) //0.26
//      aux = forecast_array.map(x => if(x.toInt > 5) true else false) //0.29
//
//    }

//    val numrows = frame_forecast.numRows
//    val columna = frame_forecast.at(0->numrows, 2)
//    for (i <- 0 until 39){
//      frame_forecast.at(Array(1, numrows-1), 3).filter(x => x.toDouble>6.0) //0.009
//    }

    val numrows = frame_forecast.numRows
    val columna = frame_forecast.at(0->numrows, 2)
    val serieColumn = frame_forecast.at(Array(1, numrows-1), 3)


    println(serieColumn)
//    val finish: Instant = Instant.now
//    val timeElapsedReadDataCSV: Long = Duration.between(start, finish).toMillis
//    println("El proceso ha tardado: " + timeElapsedReadDataCSV / 1000.0)
  }

  def aa() ={
    val forecast = CsvFile("C:\\data\\forecast.csv")
    val frame_forecast = CsvParser.parse()(forecast).withColIndex(0)
    val numrows = frame_forecast.numRows
    frame_forecast.at(Array(1, numrows), 0).filter(x => x.toInt>6)
  }

  def transformRule(rule: String): String = {
    var cleanRule = rule.replace("\r", "")
      .replace("\n", " ")

    var listParenthesis: List[String] = List()
    for(i <- 0 until cleanRule.length){
      if(cleanRule(i)=='(' | cleanRule(i)==')') {
        listParenthesis = listParenthesis :+ cleanRule(i).toString
      }
    }

    if (listParenthesis.length % 2 != 0) {println("La regla no esta bien, no estan cerrados todos los parentesis"); break}
    var contador: Int = 0
    for (j <- 1 until listParenthesis.length-2){
      if (listParenthesis(j)=="(") contador +=1
      if (listParenthesis(j)==")") contador -=1
      if (contador < 0) contador = -9999999
    }

    if(contador >= 0){
      cleanRule = cleanRule.substring(1).dropRight(1)
    }

    cleanRule
  }

}
