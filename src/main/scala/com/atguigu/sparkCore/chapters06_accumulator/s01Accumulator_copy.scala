package com.atguigu.sparkCore.chapters06_accumulator

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2}

import scala.collection.mutable

object s01Accumulator_copy {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /*  val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)
    val accumulator: LongAccumulator = sc.longAccumulator

    rdd.foreach{
      case a => accumulator.add(a)
    }

   println(accumulator.value)*/


    /*  val rdd: RDD[String] = sc.makeRDD(List("aa","bb","cc","ac","ab"),2)
    val accumulatorV = new WdAccumulatorV2
    sc.register(accumulatorV)
    rdd.foreach{
      a => accumulatorV.add(a)
    }
    //accumulate单独拿出来使用
    println(accumulatorV.value)*/




    val rdd: RDD[String] = sc.makeRDD(List("aa","bb","nn","dd","aa","ee","ff","dd","ss","dd","aa"))

    val myAccumulatorV = new MyAccumulatorV2

    sc.register(myAccumulatorV)

    rdd.foreach {
      a => myAccumulatorV.add(a)
    }

    println(myAccumulatorV.value)

  }


}


//wordcount
class MyAccumulatorV2 extends AccumulatorV2[String,mutable.HashMap[String,Int]] {

  //自定义输出结果
  val hashMap: mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()

  override def isZero: Boolean = hashMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = new MyAccumulatorV2

  override def reset(): Unit = hashMap.clear()

  override def add(v: String): Unit = {
    if (!hashMap.contains(v))
      hashMap += (v -> 0)

    hashMap.update(v, hashMap(v) + 1)

  }

  //判断是不是MyAccumulatorV2  foldLeft => 0 + (1,2,3,4,5)(_+_) => 0+1+2+3+4+5
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {




    other match {
      case acc: MyAccumulatorV2 => {
        acc.hashMap.foldLeft(this.hashMap) {
          case (map, (k, v)) => map += (k -> (map.getOrElse(k, 0) + v))
        }
      }
    }
  }


  override def value: mutable.HashMap[String, Int] = hashMap

}



class WdAccumulatorV2 extends AccumulatorV2[String,util.ArrayList[String]]{

  //确定返回结果，最终值
  val arrayList = new util.ArrayList[String]()

  override def isZero: Boolean = arrayList.isEmpty

  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = new WdAccumulatorV2

  override def reset(): Unit = arrayList.clear()

  override def add(v: String): Unit = {
    if (v.contains("c"))
      arrayList.add(v)
  }

  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    arrayList.addAll(other.value)
  }

  override def value: util.ArrayList[String] = arrayList
}






