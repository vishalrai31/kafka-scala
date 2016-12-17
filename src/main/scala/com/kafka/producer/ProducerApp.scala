package com.kafka.producer


object ProducerApp {


  def main(args: Array[String]): Unit = {
    val topic = "demo_topic"
    val producer = new KafkaProducer("localhost:9092")
    (1 to 100000).foreach{no =>
      Thread.sleep(100)
      println("Sending..............................")
      producer.send(topic, "message "+no)
    }
  }
}
