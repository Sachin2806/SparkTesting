package com.tgt.Spark.Assignments

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Program1 {
  
  def main(args: Array[String]){
    
     val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
      
    //create spark context object
    val sc = new SparkContext(conf)
      
          
      val rdd = sc.parallelize(List(1,2,3,4,5))
      val rddCollect = rdd.collect()
      
      val emptyRdd = sc.parallelize(List())
      
      println("Number of Partitions: "+rdd.partitions.length)
      println("Action: First element: "+rdd.first())
      println("Empty RDD: " + emptyRdd)
      println("Number of Partitions: "+emptyRdd.partitions.length)
      println("Action: RDD converted to Array[Int] : ")
      rddCollect.foreach(println)
     
    
  }
}