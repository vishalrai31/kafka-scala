package com.kafka.consumer

import java.util.{Properties, UUID}

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConversions._


class KafkaConsumer(groupId: String, servers: String, topics: List[String]) {

  //in milliseconds, spent waiting in poll if data is not available in the buffer
  val timeout = 10000L

  private val props = new Properties()
  props.put("bootstrap.servers", servers)
  props.put("client.id", UUID.randomUUID().toString())
  props.put("group.id", groupId)
  props.put("key.deserializer", classOf[StringDeserializer].getName)
  props.put("value.deserializer", classOf[StringDeserializer].getName)

  private val consumer = new org.apache.kafka.clients.consumer.KafkaConsumer[String, String](props)
  // subscribe topics
  consumer.subscribe(topics)


  def read(): List[String] =
    try {
      val records: ConsumerRecords[String, String] = consumer.poll(timeout)
      records.map{
        record => record.value()
        val data=record.value()
          println("receiving data in consumer>>>>>>>>>>>>>>>>"+data)
          data
      }.toList
    } catch {
      case wue: WakeupException =>
        wue.printStackTrace()
        Nil
    }
}
