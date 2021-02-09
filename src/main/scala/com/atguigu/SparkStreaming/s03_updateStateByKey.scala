package com.atguigu.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object s03_updateStateByKey {

  def main(args: Array[String]): Unit = {

    test_updateStateByKey()

  }


  def test_updateStateByKey(): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("s03_updateStateByKey").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    ssc.sparkContext.setCheckpointDir("in")

    val group_id = "test01"
    val topic = "test"
    val zkbrokes = "192.168.30.131:2181,192.168.30.132:2181,192.168.30.133:2181"

    //拿到的是kafka k，v
    val aDStream: DStream[(String, Int)] = KafkaUtils.createStream(ssc, zkbrokes, group_id, Map(topic -> 3)).flatMap(_._2.split(" ")).map((_, 1))

    val unit: DStream[(String, Int)] = aDStream.updateStateByKey {
      case (seq, op) => {
        val i = op.getOrElse(0) + seq.sum
        Option(i)
      }
    }


    //Dstream特有输出操作
    unit.print()



    //启动接收器
    ssc.start()
    //Driver等待接收器
    ssc.awaitTermination()

  }


}
