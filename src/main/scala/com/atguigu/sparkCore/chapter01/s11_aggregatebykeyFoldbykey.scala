package com.atguigu.sparkCore.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object s11_aggregatebykeyFoldbykey {

  def main(args: Array[String]): Unit = {

//    aggregatebukey()

    foldbykey()

  }


  //两个参数列表（）（）=》（） 两个逻辑一样
  def foldbykey(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val unit: RDD[(String, Int)] = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)


    val unit1: RDD[(String, Int)] = unit.foldByKey(0)(_+_)

    unit1.foreach(println)

  }

  //两个参数列表（）（）柯里化  zeroValue，分区内bykey，分区外bykey
  //zeroValue 初始值赋值，用于两两比较
  //seqop => 分区内用初始值逐步代替zeroValue
  //combOp => 分区间计算
  def aggregatebukey(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val unit: RDD[(String, Int)] = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)


    val unit1: RDD[(String, Int)] = unit.aggregateByKey(0)(math.max(_,_),_+_)

    unit1.foreach(println)

  }

}
