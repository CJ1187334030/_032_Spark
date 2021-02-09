package com.atguigu.sparkCore.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object s14_text {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //时间戳，省份，城市，用户，广告
    val unit: RDD[String] = sc.textFile("in\\agent.log")


    //（（省，广告），1）
    val unit1: RDD[((String, String), Int)] = unit.map { x =>
      val strings: Array[String] = x.split(" ")
      ((strings(1), strings(4)), 1)
    }

    //（（省，广告），sum）
    val unit2: RDD[((String, String), Int)] = unit1.reduceByKey(_+_)

    //（省，（广告，sum））
    val value: RDD[(String, (String, Int))] = unit2.map(x => (x._1._1,(x._1._2,x._2)))

    //（省，iteable（广告，sum））
    val value1: RDD[(String, Iterable[(String, Int)])] = value.groupByKey()

    //（省，iteable（广告，sum））  iterable 变数组 排序，取前三(操作内操作)   函数用{}  x => f() => ()
    val value2: RDD[(String, List[(String, Int)])] = value1.mapValues { x =>
      x.toList.sortWith((x, y) => (x._2 > y._2)).take(3)
    }

    value2.collect().foreach(println)

  }

}
