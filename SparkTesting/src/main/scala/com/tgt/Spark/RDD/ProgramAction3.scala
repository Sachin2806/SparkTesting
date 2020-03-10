package com.tgt.Spark.RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object ProgramAction3 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
                .setAppName("ProgramAction1")
                .setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val rdd1 = List("Germany India USA","USA India Russia","India Brazil Canada China")
    val rdd2 = sc.parallelize(rdd1)
    
    val wordsRdd = rdd2.flatMap(_.split(" "))   
    val pairRDD = wordsRdd.map(f=>(f,1))
    pairRDD.foreach(println)
    
    val distinctRDD = pairRDD.distinct().foreach(println)
    
    println("Sort by Key ==>")
    val sortByKeyRDD = pairRDD.sortByKey()
    sortByKeyRDD.foreach(println)
  
    println("Reduce  by Key ==>")
    val reduceByKeyRDD = pairRDD.reduceByKey(_+_)
    reduceByKeyRDD.foreach(println)
    
    println("collectAsMap ==>")
    val collectAsMapRDD = pairRDD.collectAsMap()
    collectAsMapRDD.foreach(println)
    
    
  }
}