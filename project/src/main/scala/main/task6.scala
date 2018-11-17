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
object task6 {
  
  def elbow(data:DataFrame) {
    
    var listOfRecords = new ListBuffer[Array[String]]()
    //adds field row
    val csvFields = Array("k", "value")    
    listOfRecords += csvFields
    
    for(i<- 2 to 60){
      println(i)
      listOfRecords += Array(i.toString, Kmeans(data, i).toString())
    }
    Helper.writeFile("../results/task6.csv",listOfRecords)
  }
  
  def Kmeans(data:DataFrame, K:Int):Double ={
    val distSum = 0
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
   

    //val dataRDDT=data.select("X","Y").filter("X is not null or Y is not null").rdd
    
     val newFrame=data.select("X","Y", "Vkpv").filter("X is not null or Y is not null").rdd
     
     
    
     
     def rowIn4D(row:Row):Row={
      
      val Cos=dayToCos(row.getString(2))
      val Sin=dayToSin(row.getString(2))
      //println(Cos,"//", Sin)
    
      
     return  Row.fromTuple((row.getInt(0).doubleValue(),row.getInt(1).doubleValue(),Sin, Cos))
    }
    
    val dataRDD4D=newFrame.map(rowIn4D)
    
    
    
     //val dataRDD=dataRDDT.map(row=>Row.fromTuple((row.getInt(0).doubleValue(),row.getInt(1).doubleValue())))

    
    //var randomArray= dataRDD.takeSample(false,K)
    var randomArray4D= dataRDD4D.takeSample(false,K)
    
    val loops = 0
    
    //var results=Array[(Int,(Double,Double))]((0,(0,0)))
    var results4D=Array[(Int,(Double,Double, Double, Double))]((0,(0,0,0,0)))
    
     for(loops<- 0 to 20){
      
       val newF=dataRDD4D.map(location=>(findNearest4D(location,randomArray4D), location)).groupByKey().mapValues(findCenter4D).collect()

      val test= for(k<-newF)yield {
        Row.fromTuple(k._2)
      }
       randomArray4D = test
    
     
      
      if(loops==20)results4D=newF
    }
    var centers = for(result<-results4D) yield{
      Row.fromTuple(result._2)
    }
    val distanceRDD = dataRDD4D.map(row=>findNearest4Ddist(row,centers))
    

   return distanceRDD.sum()
  }
  
    
    
  def findNearest4D(row:Row, randomArray:Array[Row]):Int={
    
    var i =0
    var minDistance= 1.7976931348623157E308
   
    var nearest=100000
    for ( i <-0 to randomArray.length -1){
       val distance=Helper.countDistance4D(row.getDouble(0), row.getDouble(1),row.getDouble(2), row.getDouble(3), randomArray(i).getDouble(0), randomArray(i).getDouble(1), randomArray(i).getDouble(2), randomArray(i).getDouble(3))
     
      if(distance<minDistance){
        minDistance=distance
        nearest=i
      }
    }
    
    
    
    return nearest
  }
  
  def findNearest4Ddist(row:Row, randomArray:Array[Row]):Double={
    
    var i =0
    var minDistance= 1.7976931348623157E308
   
    var nearest=100000
    for ( i <-0 to randomArray.length -1){
       val distance=Helper.countDistance4D(row.getDouble(0), row.getDouble(1),row.getDouble(2), row.getDouble(3), randomArray(i).getDouble(0), randomArray(i).getDouble(1), randomArray(i).getDouble(2), randomArray(i).getDouble(3))
     
      if(distance<minDistance){
        minDistance=distance
        nearest=i
      }
    }    
    
    return minDistance * minDistance
  }
  
 
  
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
