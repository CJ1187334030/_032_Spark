package com.atguigu.sparkCore.chapter01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object s07_union_subtract_intersection {

  def main(args: Array[String]): Unit = {

//    union()
//    intersetion()
    subtract()

  }

  //差集
  def subtract(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7))
    val value1: RDD[Int] = sc.parallelize(Array(5,6,7,8,9))

    val ints: Array[Int] = value.subtract(value1).collect()

    ints.foreach(println)

  }

  //交集
  def intersetion(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7))
    val value1: RDD[Int] = sc.parallelize(Array(5,6,7,8,9))

    val ints: Array[Int] = value.intersection(value1).collect()

    ints.foreach(println)

  }


  //交集
  def union(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7))
    val value1: RDD[Int] = sc.parallelize(Array(5,6,7,8,9))

    val ints: Array[Int] = value.union(value1).collect()

    ints.foreach(println)

  }

}
