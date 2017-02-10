package exercise

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

/**
  * Created by vrai on 1/26/2017.
  */
object SoccerTransformation {
  case class Soccer(firstName:String, lastName:String, country:String, matchAndScore:Map[String,Int])
  case class SoccerS(firstName:String, lastName:String, country:String, matchAndScore:Map[String,String])

def main(args:Array[String]): Unit ={

  val sparkConf=new SparkConf().setAppName("Soccerevent").setMaster("local[2]")
  val sc=new SparkContext(sparkConf)
  val rowdata=sc.textFile("D:\\Scala\\soccerevent.txt")
/*  val splitedLine=rowdata.map(line => line.split("\t")).map(line => ( (line(0).concat(":"+line(1)).concat(":"+line(2))), (line(3).concat(":"+line(4))) ) )
  val joinedRdd=splitedLine.groupByKey().map{
    case(k,v) =>
      (k,v.toList )
  }
  val mappedRdd=joinedRdd.map{
    case(k,v) =>
      (k,createDatamapList(v))
  }
  val finalRDD=mappedRdd.map{
    case(k,v) =>
      (Soccer(k.split(":")(0),k.split(":")(1),k.split(":")(2),v) )
  }*/

  val splitedLine=rowdata.map(line => line.split("\t")).map(line => ( (line(0),line(1),line(2)), (line(3),line(4)) ) )
  val joinedRdd=splitedLine.groupByKey().map{
    case(k,v) =>
      (k,v.toMap )
  }.map{
    case(k,v) => (SoccerS(k._1,k._2,k._3,v ) )
  }.foreach(println)







}
   def createDatamapList(value:List[String]): Map[String, Int] ={
    var myMap=scala.collection.immutable.HashMap[String, Int]()
    value.foreach( p =>
       myMap= myMap + (p.split(":")(0) -> p.split(":")(1).toInt)
     )
    myMap
  }


}
