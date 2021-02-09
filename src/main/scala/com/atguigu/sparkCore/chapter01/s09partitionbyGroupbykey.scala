package com.atguigu.sparkCore.chapter01

import org.apache.hadoop.mapred.lib.HashPartitioner
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object s09partitionbyGroupbykey {

  def main(args: Array[String]): Unit = {

//    partitionby()

    groupbykey()
  }

  //分区，继承HashPartitioner自定义分区规则
  def groupbykey(): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[(String, Int)] = sc.parallelize(Array(("a",1),("b",1),("c",1),("a",1)))

    val tuples: Array[(String, Int)] = value.groupByKey().map(t => (t._1,t._2.sum)).collect()

    tuples.foreach(println)

  }


  //分区，继承HashPartitioner自定义分区规则
  def partitionby(): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[(String, Int)] = sc.parallelize(Array(("a",1),("b",1),("c",1)),8)

    val value1: RDD[(String, Int)] = value.partitionBy(new org.apache.spark.HashPartitioner(2))

    value1.foreach(println)

  }

}
