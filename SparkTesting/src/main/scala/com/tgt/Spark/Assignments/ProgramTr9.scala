package com.tgt.Spark.Assignments

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
//import org.apache.spark.SparkContext.rddToPairRDDFunction
//import org.apache.spark.sql.SparkSession

object ProgramTr9 {
  
 def main(args: Array[String]){
    
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val conf = new SparkConf()
                .setAppName("Transformation")
                .setMaster("local")
                
    val sc = new SparkContext(conf)
    
    //Different ways of creating RDD's
    
    //1st : Spark Create RDD from Seq or List (using Parallelize)
    val rdd1 = sc.parallelize(Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000)))
    rdd1.foreach(println)
    
    //2nd : Create an RDD from a text file
    val rdd2 = sc.textFile("C:/Users/CSC/git/SparkTesting/SparkTesting/Files/Input/test")
    rdd2.foreach(println)
    
    //3rd : Creating from another RDD
    val rdd3 = rdd1.map(row => (row._1,row._2+1000))
    rdd3.foreach(println)
    
    //4th : Creating an empty RDD
    val rddE1 = sc.emptyRDD
    val rddE2 = sc.parallelize(Seq())
    println("Demo of emty RDD : " + rddE1)
    println("Demo of emty RDD : " + rddE2)
  }
}