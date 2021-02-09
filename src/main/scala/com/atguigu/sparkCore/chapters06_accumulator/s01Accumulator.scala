package com.atguigu.sparkCore.chapters06_accumulator

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

object s01Accumulator {

  def main(args: Array[String]): Unit = {

//    sys_accumultor()

    usewordAccumulator()

  }


  //系统累加器  累加器和函数返回值无关，对累加器做操作
  def sys_accumultor(): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val unit: RDD[Int] = sc.parallelize(List(3, 4, 5), 2)

    val accumulator: LongAccumulator = sc.longAccumulator

    unit.foreach {
      //调用add方法
      case i => accumulator.add(i)
    }

    println(accumulator.value)

  }


  //使用自定义累加器
  def usewordAccumulator(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val accumulator: wordAccumulator = new wordAccumulator

    sc.register(accumulator)

    val unit: RDD[String] = sc.parallelize(List("hello","scala" ,"hive","spark","hadoop" ), 2)

    unit.foreach{
      case word => accumulator.add(word)
    }

    println(accumulator.value)
  }


}




//自定义累加器
class wordAccumulator extends AccumulatorV2[String,util.ArrayList[String]]{

  val list: util.ArrayList[String] = new util.ArrayList[String]

  override def isZero: Boolean = list.isEmpty

  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = new wordAccumulator

  override def reset(): Unit = list.clear()

  override def add(v: String): Unit = {
    if(v.contains("h"))
      list.add(v)
  }

  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {

    list.addAll(other.value)

  }

  override def value: util.ArrayList[String] = list
}