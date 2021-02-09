package com.atguigu.sparkCore.chapter01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object s06_sortbyGlom {

  def main(args: Array[String]): Unit = {

//    sortby()
    glom()


  }

  //分区 =》 数组
  def glom(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7,4,4,3,2,2,3,4,7,1,7,3,4,5))

    val array: Array[Array[Int]] = value.glom().collect()

    array.foreach(_.foreach(println))

  }


  //默认升序  不能 _ 代替
  def sortby(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7,4,4,3,2,2,3,4,7,1,7,3,4,5))

    val ints: Array[Int] = value.sortBy(x=>x).collect()

    ints.foreach(println)

  }


}
