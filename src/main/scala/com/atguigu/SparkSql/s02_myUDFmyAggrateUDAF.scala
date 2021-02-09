package com.atguigu.SparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.{Aggregator, SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object s02_myUDFmyAggrateUDAF {


  def main(args: Array[String]): Unit = {

//    myUDF()

    MyaggregateUDAF()

  }

  //多行返一行
  def MyaggregateUDAF(): Unit ={

    val conf: SparkConf = new SparkConf().setAppName("myUDF").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val frame: DataFrame = spark.read.json("in/person.json")

    spark.udf.register("myAvg",new MyAggreFun)

    frame.createOrReplaceTempView("person")

    spark.sql("select myAvg(age) from person").show()

  }



  def myUDF(): Unit ={

    val conf: SparkConf = new SparkConf().setAppName("myUDF").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val frame: DataFrame = spark.read.json("in/person.json")

    spark.udf.register("addName",(x:String)=>"name: "+x)

    frame.createOrReplaceTempView("person")

    spark.sql("select id,addName(name),age from person").show()

  }


}


//弱类型df
class MyAggreFun extends UserDefinedAggregateFunction {

  //输入类型
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)
  }

  //计算类型
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }

  //返回类型
  override def dataType: DataType = DoubleType

  //函数稳定
  override def deterministic: Boolean = true

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //根据查询结果更新缓冲区大小
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    buffer(0) = buffer.getLong(0) + input .getLong(0)
    buffer(1) = buffer.getLong(1) + 1

  }

  //多个节点缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

  }

  //计算逻辑
  override def evaluate(buffer: Row): Any = {

    buffer.getLong(0).toDouble/buffer.getLong(1)
  }

}