package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import transformation.ColumnConstraint.{MANDATORY, NULLABLE}

/**
  * Created by vrai on 5/25/2017.
  */
object TransformMainApp extends App{

//read file location from kafka topic

  val sparkConf=new SparkConf().setAppName("transformationExtract").setMaster("local")
  val sc: SparkContext =new SparkContext(sparkConf)
  val sqlContext=new SQLContext(sc)
  import sqlContext.implicits._
  val rdd: RDD[ColumnClass] =sc.parallelize(Seq(ColumnClass("id",MANDATORY,10),ColumnClass("value",ColumnConstraint.NULLABLE,30) ) )
  rdd.toDF().show()
  //rdd.collect().foreach(println)

}

case class ColumnClass(name:String, constraint:ColumnConstraint,value:Int)