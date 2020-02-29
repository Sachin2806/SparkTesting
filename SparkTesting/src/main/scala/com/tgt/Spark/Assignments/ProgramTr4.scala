package com.tgt.Spark.Assignments

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object ProgramTr4 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
                .setAppName("Transformation")
                .setMaster("local")
                
    val sc = new SparkContext(conf)
    
    //Demo of word Count
    val rdd = sc.textFile("C:/Users/CSC/workspace/SparkTesting/Files/Input/test")
    val flatMapRDD = rdd.flatMap(line => line.split(" "))
    val mapRDD = flatMapRDD.map(line => (line, 1))
    val filterRDD = mapRDD.filter(a=> a._1.startsWith("a"))
    val wcRDD = mapRDD.reduceByKey(_ + _)
    
    flatMapRDD.saveAsTextFile("C:/Users/CSC/workspace/SparkTesting/Files/Output/WordCount/1")
    mapRDD.saveAsTextFile("C:/Users/CSC/workspace/SparkTesting/Files/Output/WordCount/2")
    filterRDD.saveAsTextFile("C:/Users/CSC/workspace/SparkTesting/Files/Output/WordCount/3")
    wcRDD.saveAsTextFile("C:/Users/CSC/workspace/SparkTesting/Files/Output/WordCount/4")
        
  }
}