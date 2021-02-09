package com.atguigu.sparkCore.chapter03_cache_checkpoint

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object chapter03_cache_checkpoint {


  def main(args: Array[String]): Unit = {

//    cache()
    checkpint()
  }


  //cache(persist) 有action触发计算到缓存中 多种：cache，disk，副本，都，off_heap
  def checkpint(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("ck")

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6))

    //默认序列化到到jvm堆空间 可以设参
    value.checkpoint()

    value.collect().foreach(println)

  }

  //cache(persist) 有action触发计算到缓存中 多种：cache，disk，副本，都，off_heap
  def cache(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6))

    //默认序列化到到jvm堆空间 可以设参
    value.persist()

    value.collect().foreach(println)

  }


}
