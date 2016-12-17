package com.kafka.producer

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer


class KafkaProducer(servers: String) {

  private val props = new Properties()
  props.put("bootstrap.servers", servers)
  props.put("acks", "all")
  props.put("retries", "5")
  props.put("key.serializer", classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[StringSerializer].getName)

  private val producer = new org.apache.kafka.clients.producer.KafkaProducer[String,String](props)

  def send(topic:String, message:String): Future[RecordMetadata] = {
    val record = new ProducerRecord[String, String](topic, message)
    producer.send(record)
  }

}

