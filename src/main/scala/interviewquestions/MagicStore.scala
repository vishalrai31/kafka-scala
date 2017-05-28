package interviewquestions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vrai on 5/21/2017.
  */
object MagicStore extends App{


  val sparkConf=new SparkConf().setAppName("magicstore").setMaster("local")
  val sc: SparkContext =new SparkContext(sparkConf)
  val sqlContext=new SQLContext(sc)

  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val compititorData=sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true").
    option("delimiter","|")
    .option("inferSchema", "true") // Automatically infer data types
    .load("D:\\Materials\\spark-Scala-Akka\\data\\ecom\\ecom_competitor_data.txt")

  val internalProductData=sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true").
    option("delimiter","|")
    .option("inferSchema", "true") // Automatically infer data types
    .load("D:\\Materials\\spark-Scala-Akka\\data\\ecom\\internal_product_data.txt")


  val sellerData=sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true").
    option("delimiter","|")
    .option("inferSchema", "true") // Automatically infer data types
    .load("D:\\Materials\\spark-Scala-Akka\\data\\ecom\\seller_data.txt")

  val sellerMap: Map[Int, String] =sellerData.map(row => (row.getAs[Int]("SellerID"),row.getAs[String]("netValue"))).collect().toMap
  val sellerBroadcast=sc.broadcast(sellerMap)

  val minPriceRDD=compititorData.map(row => (row.getAs[Int]("productId"),row ) ).reduceByKey{
    (row1,row2) =>
      if(row1.getAs[Double]("price")<  row2.getAs[Double]("price"))
        row1
      else
        row2
  }

  val internalRDD: RDD[(Int, Row)] =internalProductData.map(row => (row.getAs[Int]("ProductId"), row ) )
  val rdd: RDD[(Int, (Row, Row))] =internalRDD.join(minPriceRDD)
  rdd.map{
    case (productID,(row1,row2)) =>
      val description=sellerBroadcast.value.getOrElse(row1.getAs[Int]("SellerID"),"")
      MagicPriceData(productID,row2.getAs[Double]("price"),"",row2.getAs[Double]("price"),row2.getAs[String]("rivalName"),description)
  }.toDF().show()


  /*val df=internalProductData.map{
    (row: Row) =>
      val minPrice=groupedDF.select($"productId",$"price").where($"productId" === row.getAs[Int]("ProductId")).first()
      MagicPriceData(row.getAs[Int]("ProductId"),minPrice.getAs[Double]("price"),"",minPrice.getAs[Double]("price"),"vishal")
  }.toDF()
*/
  println("printing...")




}

case class MagicPriceData(productid:Int, msprice:Double, timestamp:String, cheapestprice:Double, rivalname:String,sellerdescription:String)
