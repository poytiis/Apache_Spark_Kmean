package main
import scala.math.sqrt
import scala.math.pow
import java.io.{BufferedWriter,FileWriter}
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.mutable.Queue
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.math.{sin, cos, Pi}

object Helper {
  def countDistance4D(x1:Double, y1:Double,z1:Double, w1:Double,x2:Double, y2:Double,z2:Double, w2:Double):Double={
    return sqrt(pow(x1-x2,2)+pow(y1-y2,2)+pow(z1-z2,2)+pow(w1-w2,2))
  }
  
  def countDistance(x1:Double, y1:Double,x2:Double, y2:Double):Double={
    
    return sqrt(pow(x1-x2,2)+pow(y1-y2,2))
  }
  def countDistanceInt(x1:Double, y1:Double,x2:Double, y2:Double):Double={
    
    return sqrt(pow(x1-x2,2)+pow(y1-y2,2))
  }
  
  def writeFile(fileName:String, listOfRecords:ListBuffer[Array[String]]) {
    val outputFile = new BufferedWriter(new
        FileWriter(fileName))
    // NO_QUOTE_CHARACTER removes " "-signs    
    val csvWriter = new CSVWriter(outputFile,',', CSVWriter.NO_QUOTE_CHARACTER)
   
    csvWriter.writeAll(listOfRecords.toList)
    outputFile.close()
  }
  
  
  def pointToDay(x:Double, y:Double):Int={
    val scale = 450000
    // Points for days
    val weekdayCoordinates = List(List(sin(2*Pi*1/7)*scale,cos(2*Pi*1/7)*scale),
        List(sin(2*Pi*2/7)*scale,cos(2*Pi*2/7)*scale),
        List(sin(2*Pi*3/7)*scale,cos(2*Pi*3/7)*scale),
        List(sin(2*Pi*4/7)*scale,cos(2*Pi*4/7)*scale),
        List(sin(2*Pi*5/7)*scale,cos(2*Pi*5/7)*scale),
        List(sin(2*Pi*6/7)*scale,cos(2*Pi*6/7)*scale),
        List(sin(2*Pi*7/7)*scale,cos(2*Pi*7/7)*scale))
    var nearest = 99999999.0
    var nearestIndex = 0
    
    // count distance to every weekday and return nearest day
    for (i <- 0 to 6){      
      val distance = countDistance(x, y, weekdayCoordinates(i)(0),weekdayCoordinates(i)(1))
      if (distance < nearest) {
        nearest = distance
        nearestIndex = i+1
      }
    }
     
    return nearestIndex
  }
  
}