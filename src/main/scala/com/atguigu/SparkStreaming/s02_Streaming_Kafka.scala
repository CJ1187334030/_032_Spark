package com.atguigu

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSparkStreaming {

  def main(args: Array[String]): Unit = {

    demo02()

  }




  def demo02(): Unit ={


    val kafkaSparkStreaming: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSparkStreaming")

    val context: StreamingContext = new StreamingContext(kafkaSparkStreaming,Seconds(5))

    val group_id = "test01"
    val topic = "test"
    val zkbrokes = "192.168.30.131:2181,192.168.30.132:2181,192.168.30.133:2181"

    val inputDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(context,zkbrokes,group_id,Map(topic -> 3))

    val value: DStream[(String, Int)] = inputDStream.flatMap(x=>x._2.split("_")).map((_,1)).reduceByKey(_+_)

    value.print()

    //启动接受器
    context.start()
    //Driver等待接收器
    context.awaitTermination()


  }


  def demo01(): Unit ={

    //1.创建SparkConf并初始化SSC
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.定义kafka参数
    val brokers = "192.168.30.131:2181,192.168.30.132:2181,192.168.30.133:2181"
    val topic = "test"
    val consumerGroup = "spark"

    //3.将kafka参数映射为map
    val kafkaParam: Map[String, String] = Map[String, String](
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
    )

    //4.通过KafkaUtil创建kafkaDSteam
    val kafkaDSteam: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      brokers,
      consumerGroup,
      Map("test"->3)
    )

    //5.对kafkaDSteam做计算（WordCount）
    kafkaDSteam.foreachRDD {
      rdd => {
        val word: RDD[String] = rdd.flatMap(_._2.split(" "))
        val wordAndOne: RDD[(String, Int)] = word.map((_, 1))
        val wordAndCount: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
        wordAndCount.collect().foreach(println)
      }
    }

    //6.启动SparkStreaming
    ssc.start()
    ssc.awaitTermination()

  }



}