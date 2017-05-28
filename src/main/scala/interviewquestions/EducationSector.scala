package interviewquestions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by vrai on 5/21/2017.
  */
object EducationSector extends App{


  val sparkConf=new SparkConf().setAppName("education").setMaster("local")
  val sc: SparkContext =new SparkContext(sparkConf)
  val sqlContext=new SQLContext(sc)

  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val questionData=sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true").
    option("delimiter","|")
    .option("inferSchema", "true") // Automatically infer data types
    .load("D:\\Materials\\spark-Scala-Akka\\data\\edutech\\question_data.txt")
  questionData.show()

  val studentConsolidatedReport=sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true").
    option("delimiter","|")
    .option("inferSchema", "true") // Automatically infer data types
    .load("D:\\Materials\\spark-Scala-Akka\\data\\edutech\\student_consolidated_report.txt")


  val studentDetailReport=sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true").
    option("delimiter","|")
    .option("inferSchema", "true") // Automatically infer data types
    .load("D:\\Materials\\spark-Scala-Akka\\data\\edutech\\student_response.txt")
studentDetailReport.show()

  val avgDF: Map[String, (Double, Double, Double)] =studentConsolidatedReport.select($"QusetionSet",$"QT",$"VB",$"RA").
    groupBy($"QusetionSet").
    agg(avg($"QT"),avg($"VB"),avg($"RA")).withColumnRenamed("avg(QT)","QT").withColumnRenamed("avg(VB)","VB").withColumnRenamed("avg(RA)","RA")
    .rdd.map{
    row =>
      (row.getAs[String]("QusetionSet"),(row.getAs[Double]("QT"), row.getAs[Double]("VB"),row.getAs[Double]("RA") ))
  }.collect().toMap

  val avgbroadcast=sc.broadcast(avgDF)


  val filteredRDD: RDD[Row] =studentConsolidatedReport.rdd.filter{ row =>
    val asd=avgbroadcast.value.get(row.getAs[String]("QusetionSet")).get
    (row.getAs[Int]("QT") < asd._1 ||  row.getAs[Int]("VB") < asd._2 || row.getAs[Int]("RA") < asd._3)
  }
  filteredRDD.collect().take(10).foreach(println)
}
