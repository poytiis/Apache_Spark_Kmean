package main

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{window, column, desc, col}
import org.apache.spark.SparkContext

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


object projectMain  {
  def main(args: Array[String]) {
       Logger.getLogger("org").setLevel(Level.OFF)
   
  
  
  val spark = SparkSession.builder()
                          .appName("Assignment")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()
                      
                          
 
  val  WholeDataFrame: DataFrame = spark.read.option("inferSchema", "true").option("header", "true").option("delimiter", ";").csv("../data/*.csv")

  if(! args.isEmpty){
    if (args.apply(0) == "-task" && args.length == 2){
      
      if(args.apply(1)=="1"){
        task1.Kmeans(WholeDataFrame)
      }
      else if(args.apply(1)=="2"){
        task2.Kmeans(WholeDataFrame)
      }
      else if(args.apply(1)=="3"){
        println("Read documentation")
      }
      else if(args.apply(1)=="4"){
        println("Read documentation")
      }
      else if(args.apply(1)=="5"){
        task5.Kmeans(WholeDataFrame,30)
      }
      else if(args.apply(1)=="6"){
        task6.elbow(WholeDataFrame)
      }
      
    }
    else {
      println("Parameters usage: -task 1")
    }   
   }
  else{
    println("Use parameters")
    
  }
  spark.stop()
    
  }
  


}