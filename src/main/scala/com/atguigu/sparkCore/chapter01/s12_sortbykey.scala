package com.atguigu.sparkCore.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object s12_sortbykey {

  def main(args: Array[String]): Unit = {

    sortbykey()

  }



  def sortbykey(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val unit: RDD[(Int, String)] = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    val tuples: Array[(Int, String)] = unit.sortByKey(false).collect()

    tuples.foreach(println)

  }

}
