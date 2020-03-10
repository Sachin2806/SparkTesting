package com.tgt.Spark.RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ProgramAction1 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
                .setAppName("ProgramAction1")
                .setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val inputRDD = sc.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))
    val listRdd  = sc.parallelize(List(1,2,3,4,5,3,2))
    
    //reduce
    
    println("Demo of reduce 1st way : " + listRdd.reduce(_+_))
    println("Demo of reduce 2nd way : " + listRdd.reduce((x, y) => (x+y)))
    println("Demo of reduce for other RDD : " + inputRDD.reduce((x,y) => ("Total", x._2 + y._2)))
   // println("Demo of redice for other RDD : " + inputRDD.reduce((x, y) => ("Total",x._2 + y._2)))
   
    //collect
    val data = listRdd.collect()
    data.foreach(print)
    
    //count, countApprox, countApproxDistinct
    println("Demo of count 								: " + listRdd.count)
    println("Demo of countApprox 					: " + listRdd.countApprox(1200))
    println("Demo of countApproxDistinct 	: " + listRdd.countApproxDistinct())
    
    //countByValue, countByValueApprox
    println("Demo of countByValue					: " + listRdd.countByValue())
    
    //first, top
    println("Demo of first								: " + inputRDD.first())
    println("Demo of first								: " + listRdd.first())
    
    println("top 		: "  +  listRdd.top(2).mkString(","))
    println("top	 	: "  +  inputRDD.top(2).mkString(","))
    
    //min and max
    
    println("min		:  "+listRdd.min())
    println("min 		:  "+inputRDD.min())
    println("max 		:  "+listRdd.max())
    println("max 		:  "+inputRDD.max())
  
    //take, takeOrdered, takeSample
    println("take : "+listRdd.take(3).mkString(","))
    println("takeOrdered : "+ listRdd.takeOrdered(3).mkString(","))
    println("takeSample	 : "+listRdd.takeSample(true, 2))
  
  }
}