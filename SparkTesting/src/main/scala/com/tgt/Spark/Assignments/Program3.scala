package com.tgt.Spark.Assignments

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object Program3 {
  
  def main(args: Array[String]){
   
   val conf = new SparkConf()
             .setAppName("WordCount")
             .setMaster("local")
             
   val sc = new SparkContext(conf)
    
   val input = sc.textFile("C:/Users/CSC/workspace/SparkAssignment/File/Input/Spark.txt")
   
   val count = input.flatMap(line ⇒ line.split(" ")).map(word ⇒ (word, 1)).reduceByKey(_ + _)       
   //count.saveAsTextFile("C:/Users/CSC/workspace/ScalaDemo/Files/Outfile")
   count.saveAsTextFile("C:/Users/CSC/workspace/SparkAssignment/File/Output")
   System.out.println("OK");
    
  }
}