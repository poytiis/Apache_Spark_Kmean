package main
import java.io.StringReader
//import com.opencsv.CSVReader

import org.apache.spark.SparkConf

import org.apache.spark.sql.functions.{window, column, desc, col}


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, IntegerType, DoubleType}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{count, sum, min, max, asc, desc, udf, to_date, avg}
import org.apache.spark.sql.functions.unix_timestamp

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.SparkSession

import com.databricks.spark.xml._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans

import java.lang.Thread
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.math.{sin, cos, Pi}
import scala.util.Random
import scala.collection.mutable.ListBuffer
object task5 {
  
  def Kmeans(data:DataFrame, K:Int){
    
    val weekdays = List("Maanantai","Tiistai","Keskiviikko", "Torstai","Perjantai","Lauantai","Sunnuntai")
        val scale = 450000
    
    
    
    def dayToCos(day:String):Double={
      
      val number= weekdays.indexOf(day) + 1
      return cos(2*Pi*number/7)*scale
    }
    
    def dayToSin(day:String):Double={
      
      val number= weekdays.indexOf(day) + 1
      return sin(2*Pi*number/7)*scale
    }
   

    
    
     val dataFrame=data.select("X","Y", "Vkpv").filter("X is not null or Y is not null").rdd
     
     
    
     //function which adds additional dimensions to X Y pair
     def rowIn4D(row:Row):Row={
      
      val Cos=dayToCos(row.getString(2))
      val Sin=dayToSin(row.getString(2))
     
    
      
     return  Row.fromTuple((row.getInt(0).doubleValue(),row.getInt(1).doubleValue(),Sin, Cos))
    }
    
     
    val dataRDD4D=dataFrame.map(rowIn4D)
    
    
    
   //takes the initial values from dataset
    var randomArray4D= dataRDD4D.takeSample(false,K)
    
    val loops = 0
    var results4D=Array[(Int,(Double,Double, Double, Double))]((0,(0,0,0,0)))
    
    //the algorithm iterates 20 times
     for(loops<- 0 to 20){
      
       val newF=dataRDD4D.map(location=>(findNearest4D(location,randomArray4D), location)).groupByKey().mapValues(findCenter4D).collect()

      val nextCenters= for(k<-newF)yield {
        Row.fromTuple(k._2)
      }
    
     randomArray4D=nextCenters
      
      if(loops==20)results4D=newF
    }
    
    
   //write the result onto the file
   val csvFields = Array("X", "Y", "Day")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvFields    
    
    for (center<-results4D) {      
      listOfRecords += Array(center._2._1.toString(),center._2._2.toString(),Helper.pointToDay(center._2._3, center._2._4).toString())
    }
    Helper.writeFile("../results/task5.csv",listOfRecords)
    
   
  }
  
  

  
  
   //finds the nearest cener point
  //param row: a row of dataRDD4D
  //param centerArray : array of center points of the iteration
  def findNearest4D(row:Row, centerArray:Array[Row]):Int={
    
    var i =0
    var minDistance= 1.7976931348623157E308
   
    var nearest=100000
    for ( i <-0 to centerArray.length -1){
       val distance=Helper.countDistance4D(row.getDouble(0), row.getDouble(1),row.getDouble(2), row.getDouble(3), centerArray(i).getDouble(0), centerArray(i).getDouble(1), centerArray(i).getDouble(2), centerArray(i).getDouble(3))
     
      if(distance<minDistance){
        minDistance=distance
        nearest=i
      }
    }
    
    
    
    return nearest
  }
  
  
 
    //calculates the mean point of the given set of points
  //param i : set of rows which have the same nearest center point
  //return tuble whrer is the mean cordinates 
    def findCenter4D(i :Iterable[Row]):(Double, Double, Double, Double)={
    var sumX=0.0
    var sumY=0.0
    var sumZ=0.0
    var sumW=0.0
    
    val iterator = i.iterator
    while(iterator.hasNext){
      val row=iterator.next
      sumX= sumX+row.getDouble(0)
      sumY= sumY + row.getDouble(1)
      sumZ= sumZ+row.getDouble(2)
      sumW = sumW +row.getDouble(3)
    }
    
    return (sumX/i.size, sumY/i.size,sumZ/i.size,sumW/i.size)
  }
}
