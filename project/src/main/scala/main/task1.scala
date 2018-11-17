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
//col("X").isNotNull || 

object task1 {
  def Kmeans(WholeDataFrame:DataFrame) {    
    //DataFrame.printSchema()
    val newFrame:DataFrame=WholeDataFrame.select("X","Y").filter("X is not null or Y is not null")
    
    //println("paivitys")
    //newFrame.printSchema()
    val vectorAssembler = new VectorAssembler().setInputCols(Array("X", "Y")).setOutputCol("features")
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(newFrame)
    val transformedTraining = pipeLine.transform(newFrame)
    //transformedTraining.show
    //println("setK")
    val kmeans = new KMeans().setK(18).setSeed(1L)
    val kmModel = kmeans.fit(transformedTraining)
    //kmModel.summary.predictions.show
    val centers=kmModel.clusterCenters
    
    
    //for(center<-centers){
    //  println(center)
    //}
    //println( kmModel.computeCost(transformedTraining))
    
    val csvFields = Array("X", "Y")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvFields    
    
    for (center<-centers) {      
      listOfRecords += Array(center(0).toString(),center(1).toString())
    }
    Helper.writeFile("../results/basic.csv",listOfRecords)
    }
    
  
}