package com.atguigu.sparkCore.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object s01_wc {


  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("worldcound").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val unit: RDD[String] = sc.textFile(args(0))

//    val value: RDD[String] = unit.flatMap(_.split(" "))

//    val value1: RDD[(String, Int)] = value.map((_,1))

//    val value2: RDD[(String, Int)] = value1.reduceByKey(_+_)

//    println(value2)

    val tuples: Array[(String, Int)] = unit.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).collect().take(20)

    tuples.foreach(println)


  }

}


