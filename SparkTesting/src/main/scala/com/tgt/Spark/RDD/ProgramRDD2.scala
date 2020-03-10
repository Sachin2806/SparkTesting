package com.tgt.Spark.RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import akka.dispatch.Foreach

object ProgramRDD2 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
               .setAppName("RDD Demo")
               .setMaster("local")
                              
    val sc = new SparkContext(conf)
    
    //This example reads all files from a single directory, creates a single RDD
    val rddFromFile = sc.textFile("C:/Users/CSC/git/SparkTesting/SparkTesting/Files/temp2/text01.csv")
    val header = rddFromFile.first()
    val realData = rddFromFile.filter(_(0) != header(0))
    val tempData = rddFromFile.filter(x => (x(0) != header(0)))
    
    rddFromFile.foreach(println)
    realData.foreach(println)
    tempData.foreach(println)
    
    println("spark read csv files from a directory into RDD")
    val rddFromFile1 = sc.textFile("C:/Users/CSC/git/SparkTesting/SparkTesting/Files/temp2/text01.csv")
    println(rddFromFile.getClass)

    val rdd = rddFromFile1.map(f=>{f.split(",")})
    //rdd.foreach(println)
    rdd.foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })
  
//    val rdds = rddFromFile.map(f => (f.split(",")))
//    rdds.foreach(f => println("Cols 1: " + f(0) + "Cols2: " + f(1)))
    
    
    
  }
}