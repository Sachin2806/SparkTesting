package com.tgt.Spark.RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.hsqldb.Expression.Collector

object combineByKey2 {
  
 def main(args: Array[String]){
    
    val conf = new SparkConf()
                  .setAppName("CombineByKey")
                  .setMaster("local")
                  
    val sc = new SparkContext(conf)
    
    type ScoreCollector = (Int, Double)
    type PersonScores = (String, (Int, Double))
   
    val score = List(("Ajay", 98.0), ("Sachin", 85.0), ("Sachin", 81.0), ("Ajay", 90.0), ("Ajay", 85.0), ("Sachin", 88.0))
    val rdd = sc.parallelize(score)
    
    //Create the combiner
    val combiner = (score:Double) => (1, score)
    
    //Function to merge the values with in a partition, Add 1 to the # of entries and 
    //score to the existing score
    val mergeValue = (collector:ScoreCollector, score:Double) => 
    {
      (collector._1 + 1, collector._2 + score)
    }
    
    //Function to merge across the partitions
    val mergeCombiners = (collector1:ScoreCollector, collector2:ScoreCollector) => 
    {
      (collector1._1 + collector2._1, collector1._2 + collector2._2)
    }
     
    //Function to calculate the average.PersonScores is a custom type
    val CalculateAvg = (personScore:PersonScores) => 
    {
      val (name,(numOfScores,score)) = personScore
      (name, score/numOfScores)
    }
    
    val rdd1=rdd.combineByKey(combiner, mergeValue, mergeCombiners).map(CalculateAvg)
    //val rdd2=rdd.combineByKey(combiner, mergeValue, mergeCombiners)
    rdd1.collect().foreach(println)
   }
  
}