package com.tgt.Spark.Assignments


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.SparkContext._

//Demo of Actions in Spark

object ProgramTr5 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
                .setAppName("Transformation")
                .setMaster("local")
                
    val sc = new SparkContext(conf)
    
    val names1 = sc.parallelize(List("abe", "abby", "apple"))
    val names2 = names1.reduce((t1,t2) => t1 + t2)
    val names3 = names1.reduce(_+_)
    
    val rdd1 = sc.parallelize(List(1,2,3)).flatMap(x=>List(x,x,x)).collect
    val rdd2 = sc.parallelize(List(1,2,3)).map(x=>List(x,x,x)).collect
    val rdd3 = sc.parallelize(List("apple", "beatty", "beatrice"))
    val rdd4 = sc.parallelize(List("twins", "brewers", "cubs", "white sox", "indians", "bad news bears"))
    val hockeyTeams = sc.parallelize(List("wild", "blackhawks", "red wings", "wild", "oilers", "whalers", "jets", "wild"))
    
    println("Demo of reduce 1st Way : " + names2)
    println("Demo of reduce 2nd Way : " + names3)
    
    println("Demo of collect        : ")
    rdd1.mkString(",").foreach(print)
    println()
    println("Demo of count	        : " + rdd3.count())
    println("Demo of first	        : " + rdd3.first())
    
    rdd3.take(2).mkString(",").foreach(print)
    rdd4.takeSample(true, 3).mkString(",").foreach(print)
    hockeyTeams.map(k => (k,1)).countByKey.foreach(print)
    
  }
}