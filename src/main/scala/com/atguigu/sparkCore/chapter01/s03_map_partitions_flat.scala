package com.atguigu.sparkCore.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object s03_map_partitions_flat {

  def main(args: Array[String]): Unit = {


//    s2_mapPartitions()

    s3_flatmap()

  }


  def s3_flatmap(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val value: RDD[List[Int]] = sc.parallelize(Array(List(1,2,3,4),List(2,3,4,6),List(1,2,3,5,5,6,7)))

    //数组合并
    val ints: Array[Int] = value.flatMap(x=>x).collect()

    ints.foreach(println)
  }


  def s2_mapPartitions(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(1 to 10)

    val ints: Array[Int] = value.mapPartitions(datas => {
      datas.map(_ * 2)
    }).collect()
    ints

    ints.foreach(println)
  }



  def s1_map(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val ints: Array[Int] = sc.parallelize(1 to 10).map(_*2).collect()

    ints.foreach(println)
  }

}

