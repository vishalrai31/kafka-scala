package com.kafka.consumer

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by vrai on 1/12/2017.
  */
object SmartMeterSparkConsumer {
  def main(args:Array[String]): Unit ={

    val zkQuorum:String="localhost:2181"
    val group="group1"
    val topics="meterreader"
    val numThreads=1

    val sparkConf = new SparkConf().setAppName("SmartMeterConsumer").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val sc=ssc.sparkContext
    ssc.checkpoint("checkpoint")
    val custAvgValue: RDD[String] =sc.textFile("D:\\EDM_Sample_Project\\sample\\Sampleavgdata.csv")

    //val topicMap:Map[String, Int] = Map("meterreader" ->1)
    val topicSet:Set[String]=Set("meterreader")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")

    //val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER_2)
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicSet)
    //(null,10006414,12/01/2017 15:48 PM,0.363)
    val streamRDD=messages.map(t => (t._2.split(",")(0), t._2) )
    //var batchRDD=custAvgValue.map(line => (line.split(",")(0),line ))
    var batchRDD=custAvgValue.map(line => (line.split(","))).map(a => (a(0),a(2))).collect().toMap
    val broadcastMap=sc.broadcast(batchRDD)
    //val temMap:Map[Int,Double]=Map((10006414 -> 0.231) )
    //(10006414,10006414,13/01/2017 16:08 PM,1.179::0.231)
    //val mapStreamRDD=streamRDD.map(line => (line._2.split(",")(0), line._2+","+temMap.get(line._2.split(",")(0).toInt).get ))
    val mapStreamRDD=streamRDD.map(line => line._2.split(",")).map(a => (a(0),a+","+broadcastMap.value(a(0)) ))
    val filteredRDD=mapStreamRDD.filter(x => isValid(x._2))

    val f2: DStream[(String, String)] = filteredRDD.map({case (x,y) => (x,y+",1")})
    println("-=-=-=-=-=-=-=-=-")
    ///(10006414,10006414,13/01/2017 15:51 PM,1.055,0.231)
    f2.reduceByKeyAndWindow(((x: String,y: String) => getFlag(x)+getFlag(y)+""),Seconds(40),Seconds(10)).print()

    ssc.start()
    ssc.awaitTermination()
  }

  def isValid(input : String) : Boolean = {
//    println("Input is.............." + input)
    val arr = input.split(",");
    val currentR = arr(2).toDouble;
    val avgR = arr(3).toDouble;
    currentR > avgR
  }

  def getFlag (input : String ) : Int = {
    println("input>>>>."+input)
    if(input.length==1)
      input.toInt
    else
    input.split(",")(4).toInt;
  }


}
