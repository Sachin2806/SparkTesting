package com.tgt.Spark.Assignments

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ProgramTr1 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
                .setAppName("Transformation")
                .setMaster("local")
                
    val sc = new SparkContext(conf)
    
    val rdd1 = sc.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014)))
    val rdd2 = sc.parallelize(Seq((5,"dec",2014),(17,"sep",2015)))
    val rdd3 = sc.parallelize(Seq((6,"dec",2011),(16,"may",2015)))
    val rdd4 = sc.parallelize(List(1,2,4,7,9,22))
    val rdd5 = sc.parallelize(List(2,11, 5, 8, 4,7,80,9))

    val rddUnion = rdd1.union(rdd2).union(rdd3)
    //val rddIntersection = rdd4.intersect(rdd5)
    
    val data = sc.textFile("C:/Users/CSC/workspace/SparkAssignment/File/Input/Spark1.txt")
    val mapFile = data.map(line => (line, line.length()))
    //val mapFile = data.map(line => line.split(" ").collect())
    
    val flatMapFile = data.flatMap(line => line.split(","))
    val filterFile = data.flatMap(line => line.split(" ").filter(value => value == "Spark" || value == "spark" ))
    
    mapFile.saveAsTextFile("C:/Users/CSC/workspace/SparkAssignment/File/Output/Map")
    flatMapFile.saveAsTextFile("C:/Users/CSC/workspace/SparkAssignment/File/Output/FlatMap")
    rddUnion.saveAsTextFile("C:/Users/CSC/workspace/SparkAssignment/File/Output/Union")
    println("Count after filtering : " + filterFile.count())
     
  }
}