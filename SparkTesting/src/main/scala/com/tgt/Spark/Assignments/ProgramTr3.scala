package com.tgt.Spark.Assignments


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object ProgramTr3 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
                .setAppName("Transformation")
                .setMaster("local")
                
    val sc = new SparkContext(conf)
    
    //Demo of map and flatMap
    val mapRDD = sc.parallelize(List(1,2,3)).flatMap(x=>List(x,x,x))
    val flatMapRDD = sc.parallelize(List(1,2,3)).map(x=>List(x,x,x))
    
    mapRDD.foreach(println)
    flatMapRDD.foreach(print)
  }
}