package Twiter

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

/**
  * Created by vrai on 1/10/2017.
  */
object FileReaderScala {

  private val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("acks", "all")
  props.put("retries", "5")
  props.put("key.serializer", classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[StringSerializer].getName)
  val topic:String="meterreader"

  private val producer = new org.apache.kafka.clients.producer.KafkaProducer[String,String](props)


  case class Lines(customerid:String, reading_date_time:String, general_supply_kwh:BigDecimal) extends Ordered[Lines] {
    import scala.math.Ordered.orderingToOrdered

    def compare(that: Lines): Int ={
      val format = new java.text.SimpleDateFormat("dd/mm/yyyy HH:mm")
      val date1=format.parse(this.reading_date_time)
      val date2=format.parse(that.reading_date_time)
      date1.compareTo(date2)

    }
  }


  def main(args:Array[String]): Unit ={

    var allLines:List[Lines]=Nil

    var file=Source.fromFile("D:\\EDM_Sample_Project\\sample\\sample2.csv").getLines()


    val str1="asd asde qwe"
    val r = str1.split(" ")

    while(file.hasNext ){
      file.next()
      var line=file.next()
        allLines ::= new Lines(line.split(",")(0), line.split(",")(1), BigDecimal(line.split(",")(4)))
    }
    //allLines.foreach(println)

    allLines.sorted.foreach(println)

    //val lines:List[String]= Source.fromFile("D:\\EDM_Sample_Project\\sample\\sample.csv").getLines().toList
    //lines.sortWith(sortByDate)


      //val record = new ProducerRecord[String, String](topic, line)
      //producer.send(record)


  }


}
