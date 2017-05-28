package Recon

import java.io.Serializable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.io.Source

/**
  * Created by vrai on 2/3/2017.
  */


object FileJoining extends {

  def main(args:Array[String]): Unit ={

    val sparkConf=new SparkConf().setAppName("Processor").setMaster("local[2]")
    val sc: SparkContext =new SparkContext(sparkConf)
    val sqlContext: SQLContext =new SQLContext(sc)

    val jsondata: String = Source.fromFile("D:\\Scala\\filejoining.json").mkString
    implicit val formats = DefaultFormats
    val parsedJson: JValue =parse(jsondata)

    val asd="".toString
    println("asd::"+asd)
    /*val aschema= StructType(Array(StructField("name",IntegerType,nullable = true)))
val asdf=  sqlContext.read.format("com.databricks.spark.csv").schema(aschema).load("D:\\Scala\\sampleFiles\\files.csv")
    asdf.first()
    asdf.rdd.isEmpty()*/


    val parsedTransposingData =(parsedJson \ "transposeFile").extract[Map[String,String]]
    //transposingFileDynamic(parsedTransposingData, sc:SparkContext,sqlContext:SQLContext)

    val parsedJoiningdata=(parsedJson \ "joinFile").extract[Map[String,String]]
    //joiningMultipleFilesEquiJoin(parsedJoiningdata,sc:SparkContext,sqlContext:SQLContext)

    val parsedGroupingdata=(parsedJson \ "grouping").extract[Map[String,String]]
    //gropingfile(parsedGroupingdata,sc,sqlContext)





  }


  def joiningMultipleFilesEquiJoin(parsedJoiningdata: Map[String,String], sc:SparkContext,sqlContext:SQLContext): Unit ={

    val alphabet: Array[Char] = "abcdefghijklmnopqrstuvwxyz".toCharArray()
    val joinColumn=parsedJoiningdata.get("joincolumn").get.toString

    val listFileLocationString: List[String] =parsedJoiningdata.filter{
      case (key,value) => key.contains("file")
    }.values.toList

    val listDataFrame: List[DataFrame] =(listFileLocationString zip alphabet).map{
      case (location, aliasName) =>
        sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(location).alias(aliasName.toString)
    }
    val joinedDF=joinAll(listDataFrame)
    joinedDF.where(joinColumn).show()

  }
def joinAll(list:List[DataFrame]):DataFrame={
  def join(list:List[DataFrame],df:DataFrame ):DataFrame={
    list match {
      case head::tail =>   join(tail, df.join(head))
      case Nil => df
    }
  }
  join(list.tail, list.head)
}



  def gropingfile(parsedGroupingdata:Map[String,String], sc:SparkContext,sqlContext:SQLContext): Unit ={

    val rawdatadf=sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(parsedGroupingdata.get("file").get)
    val groupingColumn =parsedGroupingdata.get("groupByColumn").get.toString.split(",").toList
    val aggregatedColumn: List[String] =parsedGroupingdata.get("aggregatedColumn").get.toString.split(",").toList
    val aggregateExpr: Map[String, String] =aggregatedColumn.map{
      line =>
        line.split(":")
    }.map{
      x =>
        (x(0),x(1))
    }.toMap

    rawdatadf.groupBy(groupingColumn.head,groupingColumn.tail: _*).agg(aggregateExpr).show()
  }

  def transposingFileDynamic(parsedJsonLookupFile:Map[String,String], sc:SparkContext,sqlContext:SQLContext): Unit ={

    val rawdatadf: DataFrame =sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(parsedJsonLookupFile.get("datafile").get)
    val envdata=sc.textFile(parsedJsonLookupFile.get("transposefile").get)
    val outputpath: Option[String] =parsedJsonLookupFile.get("outputpath")
    val transposeColumnList=parsedJsonLookupFile.get("transposecolumns").get.split(",").map{
      line =>
        line.split(":")
    }.map(x => x(0)).toList

    val transposeColumnMap: List[ColumnDetail] =parsedJsonLookupFile.get("transposecolumns").get.split(",").map{
      line =>
        line.split(":")
    }.map(x => ColumnDetail(x(0),x(1)))        .toList

    val schemaList: List[ColumnDetail] =Utill.inferringSchemaFromFile(rawdatadf.schema)

/*
    val schemaList: List[ColumnDetail] =parsedJsonLookupFile.get("schema").get.split(",").map{
      line =>
        line.split(":")
    }.map{
      x =>
        ColumnDetail(x(0),x(1))
    }.toList
*/


    val valuesMap: Map[String, String] = envdata.map(line => line.split("=")).filter(x => transposeColumnList.exists(y => y.equalsIgnoreCase(x(0))) )
      .map(x => (x(0),x(1))).collect().toMap

    val broadcastMap: Broadcast[Map[String, String]] =sc.broadcast(valuesMap)
    val rawdataRDD: RDD[Row] =rawdatadf.rdd

    val finalComputedRDD: RDD[List[Any]] =rawdataRDD.map(
      row =>
        mapingEachRowWithSchema(row,broadcastMap.value,schemaList,transposeColumnMap)
    )

    val computedRowRdd: RDD[Row] =    finalComputedRDD.map(x => Row.fromSeq(x))

    val combineSchemaList: List[ColumnDetail] =schemaList:::transposeColumnMap
    val schema: StructType =StructType(combineSchemaList.map{
      value =>
        createSchemaStruct(value)
    }.toArray)

    val df: DataFrame =sqlContext.createDataFrame(computedRowRdd,schema)

    outputpath match {
      case Some(filePath) => df.write.format("com.databricks.spark.csv").option("header", "true").save(filePath)
      case None => println("only dataframe created")
    }


  }

  def createSchemaStruct(columnDetail:ColumnDetail)={

    columnDetail.ctype.toUpperCase match {
      case TypeConstants.int => StructField(columnDetail.name,IntegerType,nullable = true)
      case TypeConstants.amount => StructField(columnDetail.name,DecimalType.SYSTEM_DEFAULT,nullable = true)
      case _ => StructField(columnDetail.name,StringType,nullable = true)
    }

  }

  def mapingEachRowWithSchema(row:Row,valueMap:Map[String, String],schemaList:List[ColumnDetail], transposeColumnMap:List[ColumnDetail]) ={

    val mappedRowList: List[Any] =schemaList.map{
      x =>
        x.ctype.toUpperCase match{
          case TypeConstants.int =>  row.getAs[String](x.name).toInt
          case TypeConstants.amount =>  BigDecimal.exact(row.getAs[String](x.name))
          case _  => row.getAs[String](x.name)
         }
    }

    val mappedTransposeColumnList: List[String] =transposeColumnMap.map{
      x =>
        x.ctype match{
          case _ =>  valueMap.get(x.name).get
        }
    }
    val finalMappedList=mappedRowList:::mappedTransposeColumnList
    finalMappedList
  }



//Not usable method
  def transposingFile(parsedJsonLookupFile:Map[String,String], sc:SparkContext,sqlContext:SQLContext): Unit ={

    val rawdatadf=sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(parsedJsonLookupFile.get("datafile").get)
    val envdata=sc.textFile(parsedJsonLookupFile.get("transposefile").get)
    val mappedColumns =parsedJsonLookupFile.get("transposecolumns").get.split(",").toList
    val valuesList: List[String] = envdata.map(line => line.split("=")).filter(x => mappedColumns.exists(y => y.equalsIgnoreCase(x(0))) )
      .map(x => (x(0),x(1))).values.collect().toList

    val broadcastList=sc.broadcast(valuesList)
    val rawdataRDD: RDD[Row] =rawdatadf.rdd
    val rddRowAndValue: RDD[(Row, List[String])] =rawdataRDD.map(x => (x,broadcastList.value))
    val finalRDD: RDD[transposeFileCaseClass] =rddRowAndValue.map{
      case (key, value) =>
        transposeFileCaseClass(key.get(0).asInstanceOf[Int],key.get(1).toString, key.get(2).asInstanceOf[Int], key.get(3).toString, key.get(4).toString,
          value(0),value(1)
        )
    }
    val transposedFinalDF=sqlContext.createDataFrame(finalRDD)
    transposedFinalDF.show()
  }
  case class transposeFileCaseClass(assetid:Int, currency:String, amount:Int, tradeType:String,name:String,date:String,envTradetype:String)

}





