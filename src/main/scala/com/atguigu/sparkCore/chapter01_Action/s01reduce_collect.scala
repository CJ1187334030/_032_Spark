package com.atguigu.sparkCore.chapter01_Action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object s01reduce_collect {


  def main(args: Array[String]): Unit = {

//    reduce()

//    collect()

//    count()

//    first()

//    take()

//    takeOrdered

//    aggrate()
//    fold()

//    saveAsTextFile()

    countByKey()


  }


  //foreach  打印


  //countByKey 和value无关 (2,1) (5,1)
  def countByKey(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val unit: RDD[(Int, Int)] = sc.parallelize(Array((2,4),(5,6)))

    val intToLong: collection.Map[Int, Long] = unit.countByKey()

    intToLong.foreach(println)

  }


  //aggrate 简化操作   saveAsSequenceFile（Hadoop支持的文件系统）   saveAsObjectFile（序列化成对象）
  def saveAsTextFile(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6))

    value.saveAsTextFile("out\\aa")


  }


  //aggrate 简化操作
  def fold(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6))

    val i: Int = value.fold(0)(_+_)

    println(i)

  }

  //直接int返回结果
  def aggrate(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6))

    val i: Int = value.aggregate(0)(_+_,_+_)

    println(i)

  }

  //计数
  def takeOrdered(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7,4,4,3,2,2,3,4,7,1,7,3,4,5))

    val ints: Array[Int] = value.takeOrdered(4)


    ints.foreach(println)

  }

  //取前几个
  def take(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7,4,4,3,2,2,3,4,7,1,7,3,4,5))

    val ints: Array[Int] = value.take(4)

    ints.foreach(println)

  }

  //取第一个
  def first(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7,4,4,3,2,2,3,4,7,1,7,3,4,5))

    val i: Int = value.first()

    println(i)

  }



  //计数
  def count(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7,4,4,3,2,2,3,4,7,1,7,3,4,5))

    val l: Long = value.count()

    println(l)

  }


  //只对RDD =》 array  与  parallelize && makeRDD 相反
  def collect(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7,4,4,3,2,2,3,4,7,1,7,3,4,5))

    val ints: Array[Int] = value.collect()

    ints.foreach(println)

  }


  def reduce(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,4,5,6,7,4,4,3,2,2,3,4,7,1,7,3,4,5))

    val i: Int = value.reduce(_+_)

    println(i)

  }


}
