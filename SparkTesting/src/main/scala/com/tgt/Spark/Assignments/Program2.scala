package com.tgt.Spark.Assignments

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Program2 {
  
  def main(args: Array[String]){
   
   val conf = new SparkConf()
             .setAppName("WordCount")
             .setMaster("local")
             
   val sc = new SparkContext(conf)
    
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    val b = a.keyBy(_.length)  
    b.foreach(println)
    
  }
}