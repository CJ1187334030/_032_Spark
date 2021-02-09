package com.atguigu.sparkCore.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object s02_RDD {

  def main(args: Array[String]): Unit = {

    //RDD创建方式
    val conf: SparkConf = new SparkConf().setAppName("worldcound").setMaster("local[*]")
    val test = new SparkConf().setMaster("local[*]").setAppName("test")

    val sc: SparkContext = new SparkContext(conf)

    // 从内存（集合）中创建：  makeRDD   parallelize
    val unit: RDD[Int] = sc.makeRDD(Array(1,2,3,4),4)
    val unit1: RDD[Int] = sc.parallelize(Array(1,2,3,4))

    //从外部存储创建  文件  hdfs
    val unit3: RDD[String] = sc.textFile("in",3)
    val unit4: RDD[String] = sc.textFile("hdfs://hadoop102:9000/RELEASE")


    //分区规则 parallelize makeRDD 设置准确分区默认8个  textFile 最小分区，和hadoop文件分区分片规则一致
    unit3.saveAsTextFile("out")
    unit.saveAsTextFile("out2")

//    unit.foreach(println)

  }

}
