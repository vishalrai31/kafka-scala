package Recon

import javax.script._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.codehaus.janino.Java

import scala.collection.immutable.HashMap
//import net.liftweb.json._
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.io.Source

/**
  * Created by vrai on 2/3/2017.
  */
object Processer {

  def main(args:Array[String]): Unit ={

    val sparkConf=new SparkConf().setAppName("Processor").setMaster("local[1]")
    val sc=new SparkContext(sparkConf)
    val sqlContext=new SQLContext(sc)
    val rawdatadf: DataFrame =sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("D:\\Scala\\SampleData.csv")
    val schemaDetail:Array[String] =rawdatadf.schema.fieldNames //get the schema of csv file
    val rawdataRDD: RDD[Row] =rawdatadf.rdd

    val mappedVal: RDD[Map[String, Any]] =rawdataRDD.map{ row =>
      convertRowAndSchemaToValueMap(row, schemaDetail)
    }


    val jsondata: String = Source.fromFile("D:\\Scala\\config.json").mkString
    implicit val formats = DefaultFormats
    val myjson: MyJson =parse(jsondata).extract[MyJson] //get the extracted json file

    val evalTuple: RDD[(Any, Any, Any, Any)] = mappedVal.map{
      row =>
        (NashornParser.evaluateExpression(myjson.AssetID,row),
          NashornParser.evaluateExpression(myjson.CURRENCY,row),
          NashornParser.evaluateExpression(myjson.Partyname,row),
          NashornParser.evaluateExpression(myjson.TradeAmount,row))

    }
    val evalTupleRDD: RDD[Row] =evalTuple.map{
      line =>
        Row(line._1, line._2,line._3,line._4)
    }
    //map evaluated expression with final schema

    val finalSchema= {
      val AssetID = StructField("AssetID", IntegerType, true)
      val Currency = StructField("CURRENCY", StringType, true)
      val tradeAmount = StructField("TradeAmount", DoubleType, true)
      val partyName = StructField("Partyname", StringType, true)
      new StructType(Array(AssetID, Currency, partyName,tradeAmount))
    }
    val finalL3View=sqlContext.createDataFrame(evalTupleRDD, finalSchema)
    finalL3View.printSchema()
    finalL3View.show()




  }

  def convertRowAndSchemaToValueMap(row:Row, schemaDetail:Array[String]): Map[String,Any] ={
    var myMap:Map[String,Any]=Map()

    for(i<- 0 to row.length-1) yield {
      var value=row(i) match{
        case x:Int => x.asInstanceOf[Int]
        case y:String => y.asInstanceOf[String]
      }
      myMap += schemaDetail(i) -> value

    }
    myMap
  }

}

case class ColumnSchema(ctype:String, value:String)
case class MyJson(AssetID:ColumnSchema, CURRENCY:ColumnSchema, TradeAmount:ColumnSchema, Partyname:ColumnSchema)


object NashornParser{

  def evaluateExpression(columnSchema:ColumnSchema,  mappings:Map[String,Any]): Any ={
    var scriptManager = new ScriptEngineManager();
    var nashornEngine = scriptManager.getEngineByName("nashorn");
    putBindingsIntoEngine(nashornEngine, mappings)
    var expValue =columnSchema.ctype match {
      case "integer" =>  nashornEngine.eval(columnSchema.value).asInstanceOf[Int]
      case "double"  =>   nashornEngine.eval(columnSchema.value).asInstanceOf[Double]
      case "string"  =>   nashornEngine.eval(columnSchema.value).toString
    }
    expValue
  }
  def putBindingsIntoEngine(nashornEngine: ScriptEngine , mappings:Map[String,Any]): Unit ={
    var bindings: Bindings  = new SimpleBindings();
    mappings.foreach{
      case(key, value)=> bindings.put(key, value)
    }
    nashornEngine.setBindings(bindings,ScriptContext.GLOBAL_SCOPE)

  }


}
