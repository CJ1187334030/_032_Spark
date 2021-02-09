package com.atguigu.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object s01_buikdTransfer {

  def main(args: Array[String]): Unit = {

//    getSparkSessino()

    transfer()

  }

  def transfer(): Unit = {

    val transfer: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transfer")

    val spark: SparkSession = SparkSession.builder().config(transfer).getOrCreate()

    import spark.implicits._

    val unit: RDD[(Int, String, Int)] = spark.sparkContext.parallelize(Array((1, "aa", 56), (2, "bb", 34), (3, "rr", 12)))

    //获取df
    val frame: DataFrame = unit.toDF("id", "name", "age")

    //df => ds
    val ds: Dataset[Person] = frame.as[Person]

    //ds = df
    val df: DataFrame = ds.toDF()

    //df => rdd
    val rdd: RDD[Row] = df.rdd

    rdd.foreach(x => {
      println(x.getInt(0))
    })


    //rdd => ds ，本质塞进case class 让既有结构又有类型  单行或少代码用（） ，多行或多行代码用{}
    val unit1: RDD[Person] = unit.map {
      case (id, name, age) =>
        Person(id, name, age)
    }
    val unit2: Dataset[Person] = unit1.toDS()


  }

  def getSparkSessino(): Unit ={

    val test: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")

    val session: SparkSession = SparkSession.builder().config(test).getOrCreate()

    val frame: DataFrame = session.read.json("in/person.json")

    frame.show()

    session.close()

  }

}


case class Person(id:Int,name:String,age:Int){}
