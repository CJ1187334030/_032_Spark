package com.atguigu.sparkCore.chapters06_accumulator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object s02_broadcast {

  def main(args: Array[String]): Unit = {

    test01()

  }


  //调优策略 而不是解决业务的策略
  def test01(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val unit: Broadcast[List[(Int, Int)]] = sc.broadcast(List((1,2),(4,5),(7,8)))


  }

}
