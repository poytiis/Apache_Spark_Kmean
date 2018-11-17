package main

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

import scala.util.Random
import scala.collection.mutable.ListBuffer

import java.io.{BufferedWriter,FileWriter}
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.mutable.Queue
import scala.collection.JavaConversions._
import scala.math.{sin, cos, Pi}

object task4 {
 
  def Kmeans(WholeDataFrame:DataFrame) {
    
    // *****Init for basic case. Copied from task 1*****
    //DataFrame.printSchema()
//    val newFrame:DataFrame=WholeDataFrame.select("X","Y").filter("X is not null and Y is not null")
//    //.limit(500)
//    //newFrame.printSchema()
//    val vectorAssembler = new VectorAssembler().setInputCols(Array("X", "Y")).setOutputCol("features")
//    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
//    val pipeLine = transformationPipeline.fit(newFrame)
//    val transformedTraining = pipeLine.transform(newFrame)
    //transformedTraining.show
    
    
    //*****Init for third dimension case. Copied from task 2*****
    val weekdays = List("Maanantai","Tiistai","Keskiviikko", "Torstai","Perjantai","Lauantai","Sunnuntai")
    //print(weekdays.indexOf("Torstai"))
    val newFrame:DataFrame=WholeDataFrame.select("X","Y", "Vkpv").filter("X is not null and Y is not null")
    
    val scale = 450000
    val dayToNumber = udf {(day: String) => 
      weekdays.indexOf(day) + 1
    }    
    val numberToCos = udf {(number: Int) => 
      cos(2*Pi*number/7)*scale
    }     
    val numberToSin = udf {(number: Int) => 
      sin(2*Pi*number/7)*scale
    }    
    
    val frame = newFrame.withColumn("dayNumber", dayToNumber(col("Vkpv"))).withColumn("dayX" , numberToSin(col("dayNumber"))).withColumn("dayY" , numberToCos(col("dayNumber")))
    //frame.show    
    val vectorAssembler = new VectorAssembler().setInputCols(Array("X", "Y", "dayX", "dayY")).setOutputCol("features")
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(frame)
    val transformedTraining = pipeLine.transform(frame)
    
    
    
    
    //println("alustus valmis")
    
    //*****Task 4 starts here*****
    
    //listOfRecords is ListBuffer where we gather data that we want to save to csv file
    var listOfRecords = new ListBuffer[Array[String]]()
    //adds field row to ListBuffer
    val csvFields = Array("k", "value")    
    listOfRecords += csvFields
    
    //i defines the range of k's values
    for(i <- 2 to 150){
      
    val kmeans = new KMeans().setK(i).setSeed(1L)
    //println("kmeans")
    val kmModel = kmeans.fit(transformedTraining)
    //println("kmodel")
    
    //computeCost calculates K-means cost and adds index and this cost to ListBuffer   
    listOfRecords += Array(i.toString, kmModel.computeCost(transformedTraining).toString)     
    
    }
    
    //println("silmukka valmis")
    
    //calls helper function writeFile to save calculated data
    Helper.writeFile("../results/output.csv",listOfRecords)
    
        
    
  }
}