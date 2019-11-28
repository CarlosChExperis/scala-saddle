package com.breeze
import org.saddle.Series
import org.saddle.Vec
import scala.collection.mutable.Stack

object StackUtils {
  var stack = Stack[String]()
  var stackSeries = Stack[Series[String, String]]()
  var stackVecBool = Stack[Vec[Boolean]]()
  var stackCase = Stack[String]()
  var stackIFELSE = Stack[String]()
}
