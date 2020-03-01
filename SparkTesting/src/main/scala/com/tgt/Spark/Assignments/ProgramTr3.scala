package com.tgt.Spark.Assignments


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.SparkContext._

//Demo of Transformations in Spark

object ProgramTr3 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
                .setAppName("Transformation")
                .setMaster("local")
                
    val sc = new SparkContext(conf)
    
    //groupByKey() will group the integers on the basis of same key(alphabet). 
    //After that collect() action will return all the elements of the dataset as an Array.
    val data = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
    val group1 = data.groupByKey().collect()
    
    //Demo of map and flatMap
    val mapRDD = sc.parallelize(List(1,2,3)).flatMap(x=>List(x,x,x))
    val flatMapRDD = sc.parallelize(List(1,2,3)).map(x=>List(x,x,x))
    
    val words = Array("one","two","two","four","five","six","six","eight","nine","ten")
    val data1 = sc.parallelize(words)
    val data2 = data1.map(w => (w,1)).reduceByKey(_+_)
    
     val data3 = sc.parallelize(Seq(("maths",52), ("english",75), ("science",82), ("computer",65), ("maths",85)))
     val sorted = data3.sortByKey()
     
     val rdd1 = sc.parallelize(Array("jan","feb","mar","april","may","jun"),3)
     val result = rdd1.coalesce(2)
     println("Demo of coalesce()")
     result.foreach(println)
     
     data.foreach(println)
     
     val babyNamesCSV = sc.parallelize(List(("David", 6), ("Abby", 4), ("David", 5), ("Abby", 5)))
     //val aggregateBabyName = babyNamesCSV.aggregateByKey(0)((k,v) => v.toInt+k, (v,k) => k+v)
    
     mapRDD.foreach(println)
     flatMapRDD.foreach(print)
         
     println("Demo of groupByKey()")
     group1.foreach(println)
    
     println("Demo of reduceByKey()")
     data2.foreach(println)
     
     println("Demo of sortByKey()")
     sorted.foreach(println)


  }
}