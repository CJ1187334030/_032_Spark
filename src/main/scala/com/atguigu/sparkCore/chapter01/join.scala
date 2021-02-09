package com.atguigu.sparkCore.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object join {

  def main(args: Array[String]): Unit = {

    join()

  }

  //join   k匹配，（v,v） => 元组
  def join(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[(Int, String)] = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))

    val value1: RDD[(Int, Int)] = sc.parallelize(Array((1,4),(2,5),(3,6)))

    val tuples: Array[(Int, (String, Int))] = value.join(value1).collect()

    tuples.foreach(println)

  }


}
