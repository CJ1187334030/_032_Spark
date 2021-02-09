package com.atguigu.sparkCore.chapter01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object s04_groupby_filter_sample {

  def main(args: Array[String]): Unit = {

//    groupby()

//    filter()

    sample()


  }

  //作用，抽样看看数据，抽样看数据倾斜 =》 加随机数
  def sample(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(1 to 10)

    //是否放回，取样个数，种子
    val ints: Array[Int] = value.sample(false,0.6,1).collect()

    ints.foreach(println)

  }


  def filter(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(1 to 10)

    val ints: Array[Int] = value.filter(_>2).collect()

    ints.foreach(println)

  }



  //
  def groupby(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(1 to 10)

    val unit: RDD[(Int, Iterable[Int])] = value.groupBy(_%2)

    val tuples: Array[(Int, Iterable[Int])] = unit.collect()

    tuples.foreach(println)

  }

}
