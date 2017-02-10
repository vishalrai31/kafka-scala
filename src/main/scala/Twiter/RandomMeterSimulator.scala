package Twiter

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date, Properties}

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created by vrai on 1/12/2017.
  */
object RandomMeterSimulator {
  private val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("acks", "all")
  props.put("retries", "5")
  props.put("key.serializer", classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[StringSerializer].getName)
  val topic:String="meterreader"

  private val producer = new org.apache.kafka.clients.producer.KafkaProducer[String,String](props)

  //val userid=List("10006414","9898684","10006486","10006492","10006572","11189125","11379973")
  val userid=List("10006414")
  val random = scala.util.Random

  def main (args:Array[String]): Unit ={

    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy HH:mm aa")
    val date=new Date()
    val c=Calendar.getInstance()
    c.setTime(date)

    val currentDate=dateFormatter.format(c.getTime)
    println("current:"+currentDate)

    val df2 = new DecimalFormat("#.###");
    df2.format(random.nextDouble())


     while(true){
      c.add(Calendar.MINUTE, 15)
      val incrementeddate=dateFormatter.format(c.getTime)
       for(user <- userid){
         val line=""+user+","+incrementeddate+","+df2.format(random.nextDouble()+1)
         println(line)
         val record = new ProducerRecord[String, String](topic, line)
         producer.send(record)
       }
       Thread.sleep(10000)
    }

  }

}
