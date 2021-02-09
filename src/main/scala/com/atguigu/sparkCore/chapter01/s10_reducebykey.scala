package com.atguigu.sparkCore.chapter01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object s10_reducebykey {

  def main(args: Array[String]): Unit = {

    reducebykey()
    
  }

  //reducebykey比groupbykey性能高，有预聚合
  def reducebykey(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val value: RDD[(String, Int)] = sc.parallelize(Array(("a",1),("b",1),("c",1),("a",1)))


    val tuples: Array[(String, Int)] = value.reduceByKey(_+_).collect()

    tuples.foreach(println)

  }



}
