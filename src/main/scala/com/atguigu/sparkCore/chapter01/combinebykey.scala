package com.atguigu.sparkCore.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object combinebykey {

  def main(args: Array[String]): Unit = {

    combinebykey()

  }


  def combinebykey(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input: RDD[(String, Int)] = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)

    val unit: RDD[(String, (Int, Int))] = input.combineByKey(
      (_,1),
      (acc:(Int,Int), v)=>(acc._1+v,acc._2+1), (acc1:(Int,Int),
                                                acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))


    val unit1: RDD[(String, Double)] = unit.map {
      case (a, b) => (a, b._1.toDouble / b._2.toDouble)
    }

    unit1.foreach(println)

  }

}
