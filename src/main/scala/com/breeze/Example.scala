package com.breeze

import java.sql.Timestamp
import java.time.Instant
import java.time.Duration
import java.util.{Calendar, Date}

import org.saddle.io.{CsvFile, CsvParser}
import org.saddle.{Frame, Panel, Series, Vec}

object Example {
//  def main(args: Array[String]): Unit = {
//
//    val start: Instant = Instant.now
//    val forecast = CsvFile("C:\\data\\forecast.csv")
//    val cells = CsvFile("C:\\data\\cells.csv")
//    val bottlenecks = CsvFile("C:\\data\\bottlenecks.csv")
//    val finish: Instant = Instant.now
//    val timeElapsedReadDataCSV: Long = Duration.between(start, finish).toMillis
//    println("El proceso ha tardado: " + timeElapsedReadDataCSV / 1000.0)
//
//    println(bottlenecks)
//
//    val start2: Instant = Instant.now
//    //val frame = CsvParser.parse()(file).withRowIndex(0).withColIndex(0)
//    val frame_forecast = CsvParser.parse()(forecast).withColIndex(0)
//    val frame_cells = CsvParser.parse()(cells).withColIndex(0)
//    val frame_bottleneck = CsvParser.parse()(bottlenecks).withColIndex(0)
//
////    val lista3: ArrayList[String] = new ArrayList[String]()
////    val lista2 = List(List("a", 3, 4), List("e", 10, 6))
////    // parse columns 2 and 9 of the CSV and convert the result to a Frame
////    // (we know in advance these cols are candidate name and donation amount)
////    // & set the first row as the col index
////    // & the first col (candidate names) as the row index
////    val start: Instant = Instant.now
////    val lista: Array[String] = Array("traffic_dl_kbps", "date")
////    val framefilter = frame.col("traffic_dl_kbps", "date")
////    val framefilter2 = frame_forecast.col(lista)
////    print(frame_forecast)
////    print(frame_cells)
//
////    println(frame_cells.filterIx(_>=2))
////    println(frame_cells.filter())
////    println(frame_forecast.join(frame_cells, how = OuterJoin))
//
////    println(frame_forecast.col("id.cell")== frame_cells.col("id.cell"))
//    //    println(frame.col("traffic_dl_kbps", "date"))
////    println(frame.col(lista))
////    println(frame.col("date"))
//
//    val b = Series(Vec(5,2,1), Index("b","b","d"))
//    val a = Series(Vec(1,4,2), Index("a","b","b"))
//
//    val ab = Frame("x" -> a, "y" -> b)
//    val ba = Frame("w" -> a, "z" -> b)
//
////    println(ab.toSeq.toArray)
//
//    val aux = ab.join(ba)
////    val aux2 = aux.setColIndex(Index("x", "y", "w", "z"))
////    println(aux2)
//
////    println("LeftJoin")
////    println(a.join(b, how=LeftJoin))
////    println("RightJoin")
////    println(a.join(b, how=RightJoin))
////    println("InnerJoin")
////    println(a.join(b, how=InnerJoin))
////    println("OuterJoin")
////    println(a.join(b, how=OuterJoin))
//
//    val finish2: Instant = Instant.now
//    val timeElapsedReadDataCSV2: Long = Duration.between(start2, finish2).toMillis
//    println("El proceso ha tardado: " + timeElapsedReadDataCSV2 / 1000.0)
//  }

  def main(args: Array[String]): Unit = {
    val forecast = CsvFile("C:\\data\\forecast.csv")
    val frame_forecast = CsvParser.parse()(forecast).withColIndex(0)
    println(frame_forecast)

    val aux = (("hola", 2, "ee"), ("ee", 3, "ii"))
    val ii = List("a", "b", "c")
    val ee2 = Series("x" ->aux)
    val uu2 = Series("x" ->aux)
    println(Frame(ee2))
    val array1 = Vec(Array("a", "b", "c", "d", "e", "f"):_*)
    val array2 = Vec(Array(4.0, 1.0, 2.0, 7.0,5.0, 4.0):_*)
    val serie_array1 = Series(array1)
    val serie_array2 = Series(array2)
//    val p = Panel(Vec(1,2,3), Vec("a","b","c"))
    println(
      Panel("a" -> serie_array1, "b" -> serie_array2)
    )
    println(Series(array1))

//    val start: Instant = Instant.now
//    println(getdfForecast(array1, array1, array2, array2, array2,
//      array2, array2, array2, array2, array2,
//      array2, array2, array2, array2, array2,
//      array2, array2, array2, array2, array2
//    ))
//    val finish: Instant = Instant.now
//    val timeElapsedReadDataCSV: Long = Duration.between(start, finish).toMillis
//    println("El proceso ha tardado: " + timeElapsedReadDataCSV / 1000.0)

    getFrameRAuxiliartres
  }

  def getFrameR:List[(Int, String, String)] = {
    val forecast = CsvFile("C:/data/forecast.csv")
//    val cells = CsvFile("C:\\data\\cells.csv")
//    val bottlenecks = CsvFile("C:\\data\\bottlenecks.csv")

    val frame_forecast = CsvParser.parse()(forecast).withColIndex(0)
//    val frame_cells = CsvParser.parse()(cells).withColIndex(0)
//    val frame_bottleneck = CsvParser.parse()(bottlenecks).withColIndex(0)

    frame_forecast.toSeq.toList
  }

//  def getFrameRAuxiliar: Frame[String, String, Int] = {
  def getFrameRAuxiliar:(String, Int, String) = {
    val s = Series("a" -> 1, "b" -> 2)
    val t = Series("b" -> 3, "c" -> 4)

    Frame("x" -> s, "y" -> t)
    ("hola", 2, "hola")
  }

  def getFrameRAuxiliardos: List[(String, String, Int)] = {
    val s = Series("a" -> 1, "b" -> 2)
    val t = Series("b" -> 3, "c" -> 4)

    Frame("x" -> s, "y" -> t).toSeq.toList
  }

  def getFrameRAuxiliartres: Array[(String, String, Int)] = {
    val s = Series("a" -> 1, "b" -> 2)
    val t = Series("b" -> 3, "c" -> 4)


    val prueba2 = Frame("x" -> s, "y" -> t).toSeq.toArray
    for (i <- 0 to prueba2.length)
    {println(prueba2(i)._1)
    }
    prueba2
  }

  def getArrayR: Array[Array[String]] = {
    val prueba2 = Array(Array("hola", "carlos", "uuuu"), Array("buneas", "tardes", "todos"))
    prueba2
  }

  def getListR: List[String] = {
    List("hola", "carlos", "uuuu")
  }

  def getdataframeR(df: Array[String]): Array[String] = {
    df
  }

//  def getdataframeR2(df: Array[String], df1: Array[Double]) = {
//    val serie_aux = Series("x" -> df, "y" -> df1)
//    Frame(serie_aux).toSeq.toArray
//  }

  def getdataframeR3(df: Array[Double]): Array[Double] = {
    df
  }

  def getdataframeR4(df: Array[Double]): Array[(Int, Int, Array[Double])] = {
    Frame(Series(df)).toSeq.toArray
  }

  def getdfForecast(id_cell: Array[String], date: Array[Timestamp], voice_erlang: Array[Double],
                    traffic_dl_kbps: Array[Double], traffic_ul_kbps: Array[Double],
                    RRC_connected_users: Array[Double], DL_active_users: Array[Double],
                    UL_active_users: Array[Double], DL_user_thp: Array[Double], UL_user_thp: Array[Double],
                    DL_spec_eff: Array[Double], UL_spec_eff: Array[Double], DL_PRB_util: Array[Double],
                    UL_PRB_util: Array[Double], DL_Sched_Ent: Array[Double], UL_Sched_Ent: Array[Double],
                    RSSI: Array[Double], DL_CET: Array[Double], UL_CET: Array[Double],
                    CQI: Array[Double]):Frame[Int, String, Any] = {

    val serieIdCell = Series(Vec(id_cell:_*))
    val serieDate = Series(Vec(date:_*))
    val serieVoiceErlang = Series(Vec(voice_erlang:_*))
    val serieTrafficDlKbps = Series(Vec(traffic_dl_kbps:_*))
    val serieTrafficUlKbps = Series(Vec(traffic_ul_kbps:_*))

    val serieRRCConnectedUsers = Series(Vec(RRC_connected_users:_*))
    val serieDLActiveUsers = Series(Vec(DL_active_users:_*))
    val serieULActiveUsers = Series(Vec(UL_active_users:_*))
    val serieDLUserThp = Series(Vec(DL_user_thp:_*))
    val serieULUserThp = Series(Vec(UL_user_thp:_*))

    val serieDLSpecEff = Series(Vec(DL_spec_eff:_*))
    val serieULSpecEff = Series(Vec(UL_spec_eff:_*))
    val serieDLPRBUtil = Series(Vec(DL_PRB_util:_*))
    val serieULPRBUtil = Series(Vec(UL_PRB_util:_*))
    val serieDLSchedEnt = Series(Vec(DL_Sched_Ent:_*))

    val serieULSchedEnt = Series(Vec(UL_Sched_Ent:_*))
    val serieRSSI = Series(Vec(RSSI:_*))
    val serieDLCET = Series(Vec(DL_CET:_*))
    val serieULCET = Series(Vec(UL_CET:_*))
    val serieCQI = Series(Vec(CQI:_*))

    Panel(
      "id_cell" -> serieIdCell, "date" -> serieDate, "voice_erlang"-> serieVoiceErlang,
      "traffic_dl_kbps" -> serieTrafficDlKbps, "traffic_ul_kbps" -> serieTrafficUlKbps,
      "RRC_connected_users"-> serieRRCConnectedUsers,
      "DL_active_users" -> serieDLActiveUsers, "UL_active_users" -> serieULActiveUsers,
      "DL_user_thp"-> serieDLUserThp, "UL_user_thp" -> serieULUserThp, "DL_spec_eff" -> serieDLSpecEff,
      "UL_spec_eff"-> serieULSpecEff, "DL_PRB_util" -> serieDLPRBUtil, "UL_PRB_util" -> serieULPRBUtil,
      "DL_Sched_Ent" -> serieDLSchedEnt, "UL_Sched_Ent"-> serieULSchedEnt, "RSSI" -> serieRSSI,
      "DL_CET" -> serieDLCET, "UL_CET"-> serieULCET, "CQI" -> serieCQI
    )

//      .head._3.map(x=>println(x))
  }


  def getdfDate(date: Array[Timestamp]):Frame[Int, String, Any] = {

    val serieDate = Series(Vec(date:_*))

    Panel("date" -> serieDate)

  }

}
