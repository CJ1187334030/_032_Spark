package com.atguigu.sparkCore.chapter01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object s08cartesianZip {

  def main(args: Array[String]): Unit = {

//    cartesian()
    zip()


  }


  //zip 不同scala  对不上报错
  def zip(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6))
    val value1: RDD[Int] = sc.parallelize(Array(5,6,7,8,9))

    val tuples: Array[(Int, Int)] = value.zip(value1).collect()

    tuples.foreach(println)

  }

  //笛卡尔集合  10x10=100
  def cartesian(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7))
    val value1: RDD[Int] = sc.parallelize(Array(5,6,7,8,9))

    val tuples: Array[(Int, Int)] = value.cartesian(value1).collect()

    tuples.foreach(println)

  }

}
