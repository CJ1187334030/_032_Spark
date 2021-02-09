package com.atguigu.sparkCore.chapter04_File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object s01 {

  def main(args: Array[String]): Unit = {

    json()

  }

  def json(): Unit ={


    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val unit: RDD[String] = sc.textFile("in/person.json")

    val unit1: RDD[Option[Any]] = unit.map(JSON.parseFull)

    unit1.foreach(println)


  }
}
