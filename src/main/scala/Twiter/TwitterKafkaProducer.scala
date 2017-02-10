package Twiter

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created by vrai on 1/6/2017.
  */
object TwitterKafkaProducer {
  private val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092,localhost:9093")
  props.put("acks", "all")
  props.put("retries", "5")
  props.put("key.serializer", classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[StringSerializer].getName)
  val topic:String="multibroker"
  private val producer = new org.apache.kafka.clients.producer.KafkaProducer[String,String](props)

  def send(message:String): Future[RecordMetadata] ={
    val record = new ProducerRecord[String, String](topic, message)
    producer.send(record)
  }
}
