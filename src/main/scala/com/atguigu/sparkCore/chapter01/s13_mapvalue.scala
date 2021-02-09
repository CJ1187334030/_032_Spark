package com.atguigu.sparkCore.chapter01

import org.apache.spark.{SparkConf, SparkContext}

object s13_mapvalue {

  def main(args: Array[String]): Unit = {

    mapvalue()
  }

  //只对value操作
  def mapvalue(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val tuples: Array[(Int, String)] = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c"))).mapValues(_+"----haaaaaaa.....").collect()

    tuples.foreach(println)

  }

}
