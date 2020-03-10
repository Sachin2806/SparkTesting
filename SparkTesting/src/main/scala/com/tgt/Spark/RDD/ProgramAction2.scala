package com.tgt.Spark.RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ProgramAction2 {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
                .setAppName("ProgramAction1")
                .setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val inputRDD = sc.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))
    val listRdd  = sc.parallelize(List(1,2,3,4,5,3,2))
    val numbers = sc.parallelize(List(5, 4, 8, 6, 2))
    
    //aggregate on listRDD 
    def param0= (accu:Int, v:Int) => accu + v
    def param1= (accu1:Int,accu2:Int) => accu1 + accu2
    
    println("aggregate on listRDD	: "+listRdd.aggregate(0)(param0,param1))
    
    //aggregate on inputRDD
    def param2 = (accu:Int, v:(String, Int)) => accu + v._2
    def param3 = (accu1:Int,accu2:Int) => accu1 + accu2
    
    println("aggregate on inputRDD : "+ inputRDD.aggregate(0)(param2, param3))
    
    //treeAggregate on listRDD 
    def param4 = (accu:Int, v:Int) => accu + v
    def param5 = (accu1:Int,accu2:Int) => accu1 + accu2
    
    println("treeAggregate on listRDD	: "+listRdd.treeAggregate(0)(param4, param5))
    
    //fold
    println("fold : " + listRdd.fold(0){ (acc, v) =>  val sum = acc + v 
      sum    
    })
    
    println("fold :  "+inputRDD.fold(("Total",0)){(acc:(String,Int),v:(String,Int))=>
    val sum = acc._2 + v._2
    ("Total",sum)
  })
  
    println("fold sample 3 :  " + numbers.fold(0){(a,b) => a + b })
    println("Partitions in inputRDD : "  + inputRDD.partitions.length)
    println("Partitions in listRdd  : "   + listRdd.partitions.length)
    
    println("Total in fold : " + listRdd.fold(0)((x, y) => (x + y)))
    println("Min in listRDD : "+listRdd.fold(0)((x,y) => {x min y}))
    println("Max in listRDD : "+listRdd.fold(0)((x,y) => {x max y})) 
    
    println("Total in inputRDD : " + inputRDD.fold(("",0))( (acc,ele)=>{ ("Total", acc._2 + ele._2)  }))
    println("Min in inputRDD   : " + inputRDD.fold(("",0))( (acc,ele)=>{ ("Min", acc._2 min ele._2)  }))
    println("Max in inputRDD   : " + inputRDD.fold(("",0))( (acc,ele)=>{ ("Max", acc._2 max ele._2)  }))
  }
}