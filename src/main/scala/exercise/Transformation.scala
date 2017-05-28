package exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by vrai on 4/23/2017.
  */
class Transformation {


}
object Transformation{
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("ziptest").setMaster("local")
    val sc: SparkContext =new SparkContext(sparkConf)
    val pairs=sc.parallelize(Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D"))
    val tuple: RDD[(String, String)] =pairs.map{
      x =>
        (x.split("=")(0),x.split("=")(1))
    }
    tuple.reduceByKey(_+_).collect().foreach(println)

    tuple.map{
      case (key,value) =>
        (key,1)
    }.reduceByKey(_+_).collect().foreach(println)

    val zerovalue=0
    val combiner= (value:Int,key:String) => value+1
      val merger= (value1:Int,value2:Int) => value1+value2

    tuple.aggregateByKey(0)(combiner,merger).collect().foreach(println)

    val zerovalue2=mutable.HashSet.empty[String]
      val combiner2=(s:mutable.HashSet[String],value:String) => s+=value
        val merger2=(set1:mutable.HashSet[String],set2:mutable.HashSet[String]) => set1 ++= set2

    tuple.aggregateByKey(zerovalue2)(combiner2,merger2).foreach(println)


  }

}
