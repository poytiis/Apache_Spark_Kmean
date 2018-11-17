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
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans

import java.lang.Thread
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.util.Random
import scala.collection.mutable.ListBuffer
import scala.math.{sin, cos, Pi}



object task2 {
  def Kmeans(WholeDataFrame:DataFrame) {
    // List for weekdays
    val weekdays = List("Maanantai","Tiistai","Keskiviikko", "Torstai","Perjantai","Lauantai","Sunnuntai")
    
    // Create new dataframe with columns x,y,weekday and filter bad values
    val newFrame:DataFrame=WholeDataFrame.select("X","Y", "Vkpv").filter("X is not null or Y is not null")
    
    // Functions to change weekdays to scaled coordinates
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
    
    // Create day coordinates with above functions
    val frame = newFrame.withColumn("dayNumber", dayToNumber(col("Vkpv"))).withColumn("dayX" , numberToSin(col("dayNumber"))).withColumn("dayY" , numberToCos(col("dayNumber")))
    frame.show
    // Make features
    val vectorAssembler = new VectorAssembler().setInputCols(Array("X", "Y", "dayX", "dayY")).setOutputCol("features")
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(frame)
    val transformedTraining = pipeLine.transform(frame)
    
    // Find cluster centers based on features
    val kmeans = new KMeans().setK(18).setSeed(1L)
    val kmModel = kmeans.fit(transformedTraining)
    //kmModel.summary.predictions.show
    val centers=kmModel.clusterCenters
    
    // Put cluster centers to array
    val csvFields = Array("X", "Y", "Day")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvFields  
    for (center<-centers) {      
      listOfRecords += Array(center(0).toString(),center(1).toString(),Helper.pointToDay(center(2), center(3)).toString())
    }
    // Write to output file
    Helper.writeFile("../results/task2.csv",listOfRecords)
  }

}