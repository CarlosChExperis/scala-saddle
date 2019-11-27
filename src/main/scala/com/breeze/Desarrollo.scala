package com.breeze

import org.saddle.index.{InnerJoin, LeftJoin, OuterJoin}
import org.saddle.io.{CsvFile, CsvParser}
import org.saddle.{Frame, Panel, Series, Vec, Index}
import java.time.Instant
import java.time.Duration
import scala.io.Source

object Desarrollo {

  def main(args: Array[String]): Unit = {

    var forecast_array:Array[Array[String]] = Array()
    val bufferedSourceForecast = io.Source.fromFile("C:\\data\\forecast.csv")
    for (line <- bufferedSourceForecast.getLines) {
      forecast_array = forecast_array :+ line.split(",").map(_.trim)
    }
    bufferedSourceForecast.close

    var cells_array:Array[Array[String]] = Array()
    val bufferedSourceCells = io.Source.fromFile("C:\\data\\cells.csv")
    for (line <- bufferedSourceCells.getLines) {
      cells_array = cells_array :+ line.split(",").map(_.trim)
    }
    bufferedSourceCells.close

    val forecast = CsvFile("C:\\data\\forecast.csv")
    val cells = CsvFile("C:\\data\\cells.csv")
    val bottlenecks = CsvFile("C:\\data\\bottlenecks.csv")

    val frame_forecast = CsvParser.parse()(forecast).withRowIndex(0).withColIndex(0)
    val frame_cells = CsvParser.parse()(cells).withRowIndex(0).withColIndex(0)
    val frame_bottleneck = CsvParser.parse()(bottlenecks).withRowIndex(0).withColIndex(0)

    val tech = Source.fromFile("C:\\data\\technology.txt").getLines.mkString.trim
    val rule = Source.fromFile("C:\\data\\rule.txt").mkString.replaceAll("\r\n", " ")

    val frame_forecast_cells = frame_forecast.join(frame_cells, how = LeftJoin)

//    println("join forecast cells")
//    println(frame_forecast.join(frame_cells, how = LeftJoin))

    val lista_bool: List[Boolean] = List.fill(frame_bottleneck.numRows)(true)
    val frame_bool = Panel(Vec(lista_bool:_*))

    frame_forecast.joinMap(frame_cells,chow=LeftJoin){ case (x, y) => x + y }

    val s1 = Series(0 -> 1, 1 -> 2, 2 -> 3)
    val s2 = Series(0 -> 4, 1 -> 5, 2 -> 6)

    val s4 = Series(0 -> 10, 1 -> 20, 2 -> 30)
    val s5 = Series(0 -> 1, 1 -> 2, 2 -> 3)
    val s3 = Series(0 -> "id_uno", 1 -> "id_dos", 2 -> "id_tres")


    val frame1 = Panel("cabecera_uno" -> s3, "cabecera_dos" -> s2, "cabecera_tres" -> s1)
    val frame2 = Panel("cabecera_uno" -> s3, "cabecera_tres" -> s5, "cabecera_cuatro" -> s4)

//    val new_names_col = Array("cabecera_cuatro", "cabecera_cinco", "cabecera_seix")
//    println(frame1.setColIndex(Index(new_names_col:_*)))

//    //meter indices
//    val indices_panel = Array("indice1", "indice2", "indice3", "indice4")
//    val q = Series(Vec(1,3,2,4), Index(indices_panel:_*))
//    println(q)

//Agregar una columna a un Panel
//    val lista_bool2: List[Boolean] = List.fill(3)(true)
//    val frame_bool2 = Panel(Vec(lista_bool2:_*))
//    println("frame1 join frame2")
//    println(frame1.join(frame_bool2, how=LeftJoin))


  }

}
