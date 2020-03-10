package com.tgt.Spark.Assignments

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

//Demo of Actions in Spark

object ProgramTr7 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
                .setAppName("Transformation")
                .setMaster("local")
                
    val sc = new SparkContext(conf)
    
    val a = sc.parallelize(1 to 10, 2)
    val b = a.flatMap(1 to _)
    val c = a.map(1 to _)
    val numbers = 1 to 10
    val numbersRDD = sc.parallelize(numbers)
    
    val a1 = sc.parallelize(List("apple", "orange", "grape", "fig"))
    val a2 = sc.parallelize(List("mango", "pineapple", "banana"))
    
    val b1 = a1.keyBy(_.length())
    val b2 = a2.keyBy(_.length())
    
//    val c1 = b1.subtractByKey(other)
    
    println("Print each element of the original RDD : ")
    numbersRDD.foreach(println)
    
    //Performing an operation
    val doubleRDD = numbersRDD.map(n => n.toDouble / 5)
    val arrayValues = doubleRDD.collect
    
    println("Print value after computation : ")
    arrayValues.foreach(println)
    
    a.foreach(println)
    b.foreach(println)
    c.foreach(println)
    
  }
}