package com.atguigu.sparkCore.chapter01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object s05_distinctCoalesceRepartions {


  def main(args: Array[String]): Unit = {


//    distinct()

//    distinct_param()

//    coalesce()

    repations()

  }




  //重新分区  = coalesce（3，true）  shuffle =true（默认）
  def repations(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7,4,4,3,2,2,3,4,7,1,7,3,4,5))
    println(value.partitions.size)

    val value1: RDD[Int] = value.repartition(3)
    println(value1.partitions.size)

  }



  //coalesce = true（默认）
  //缩减分区，大数据集过滤后，提高小数据集执行速度
  //分区合并
  def coalesce(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7,4,4,3,2,2,3,4,7,1,7,3,4,5))
    println(value.partitions.size)

    val value1: RDD[Int] = value.coalesce(3)
    println(value1.partitions.size)

  }



  def distinct_param(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7,4,4,3,2,2,3,4,7,1,7,3,4,5))

    val ints = value.distinct(2).collect()

    ints.foreach(println)

  }


  def distinct(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7,4,4,3,2,2,3,4,7,1,7,3,4,5))

    val ints = value.distinct().collect()

    ints.foreach(println)

  }


}
