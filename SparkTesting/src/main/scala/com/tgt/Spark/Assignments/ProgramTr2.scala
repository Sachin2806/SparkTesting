package com.tgt.Spark.Assignments

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object ProgramTr2 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
                .setAppName("Transformation")
                .setMaster("local")
                
    val sc = new SparkContext(conf)
    
    //Demo of word Count
    val wc1 = sc.textFile("C:/Users/CSC/workspace/SparkAssignment/File/Input/Spark.txt")
    val wc2 = wc1.flatMap(line => line.split(""))
    val wc3 = wc2.map(word => (word,1))
    val wc4 = wc3.reduceByKey((total, value) => total + value)
    //Alternate way of using reduceByKey
    val wc5 = wc3.reduceByKey(_+_)
    wc4.saveAsTextFile("C:/Users/CSC/workspace/SparkAssignment/File/Output/WordCount/1")
    wc5.saveAsTextFile("C:/Users/CSC/workspace/SparkAssignment/File/Output/WordCount/2")
        
  }
}