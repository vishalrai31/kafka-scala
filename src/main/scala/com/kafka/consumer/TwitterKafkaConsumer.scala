package com.kafka.consumer

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


/**
  * Created by vrai on 1/7/2017.
  */
object TwitterKafkaConsumer {

  def main(args:Array[String]): Unit ={

    val zkQuorum:String="localhost:2181"
    val group="group1"
    val topics="multibroker"
    val numThreads=1

    val sparkConf = new SparkConf().setAppName("TwitterKafkaConsumer").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("checkpoint")
    val topicMap:Map[String, Int] = Map("multibroker" ->1)
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER_2)
    lines.map( line=> println("Key:"+line._1 +"::value:"+line._2) ).print()


    ssc.start()
    ssc.awaitTermination()
  }
}
