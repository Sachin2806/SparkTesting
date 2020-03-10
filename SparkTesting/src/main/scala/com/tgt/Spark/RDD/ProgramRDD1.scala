package com.tgt.Spark.RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ProgramRDD1 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
               .setAppName("RDD Demo")
               .setMaster("local")
                              
    val sc = new SparkContext(conf)
    
    //This example reads all .csv files from a single directory, creates a single RDD
    val rdds = sc.textFile("C:/Users/CSC/git/SparkTesting/SparkTesting/Files/Input/tmp")
    rdds.foreach(println)
    
    //wholeTextFiles() method reads all files and this returns an RDD[Tuple2]. where first value (_1) 
    //in a tuple is a file name and second value (_2) is content of the file.
    val rddm = sc.wholeTextFiles("C:/Users/CSC/git/SparkTesting/SparkTesting/Files/Input/tmp")
    rddm.foreach(f => println(f._1, f._2))
    rddm.foreach(println)
    
    //Spark Read multiple text files into a single RDD
    val rddmfl = sc.textFile("C:/Users/CSC/git/SparkTesting/SparkTesting/Files/Input/tmp/text01.txt,C:/Users/CSC/git/SparkTesting/SparkTesting/Files/Input/tmp/text02.txt")
    rddmfl.foreach(println)
    
    //Read all text files matching a pattern to single RDD
    val rddmp = sc.textFile("C:/Users/CSC/git/SparkTesting/SparkTesting/Files/Input/tmp/text*.txt")
    rddmp.foreach(println)
    
    //This example reads all files from multiple directories, creates a single RDD
    val rddmd = sc.textFile("D:/temp1/dir1,D:/temp1/dir2")
    rddmd.foreach(println)
    
  }
}