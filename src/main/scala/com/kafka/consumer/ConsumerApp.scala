package com.kafka.consumer


object ConsumerApp {

  def main(args: Array[String]): Unit = {
    val topic = "meterreader"
    val groupId= "demo_groupid"
    val consumer = new KafkaConsumer(groupId, "localhost:9092", List(topic))
    while (true) {
      Thread.sleep(100)
      println("Reading...............")
      consumer.read()
    }
  }

}
