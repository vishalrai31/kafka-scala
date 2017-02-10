package exercise
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vrai on 1/26/2017.
  */
object CustomerTrans {

  def main(args:Array[String]): Unit ={
    val sparkConf=new SparkConf().setAppName("Soccerevent").setMaster("local[2]")
    val sc=new SparkContext(sparkConf)
    val sqlcontext=new SQLContext(sc)
    val custdata=sc.textFile("D:\\Scala\\customerfile.txt")
    val transdata=sc.textFile("D:\\Scala\\transaction.txt")

    val custSchemaString="customer_id,email_id,language,location"
    val transSchemaString="transaction_id,product_id,customer_id,sell_price,item_description"

    val custSchema=StructType(custSchemaString.split(",").map(fieldname => StructField(fieldname, StringType,true)) )
    val transSchema=StructType(transSchemaString.split(",").map(fieldname => StructField(fieldname, StringType,true)) )





    val custRDD=custdata.map(line => line.split("\t")).map(x => Row(x(0).trim, x(1).trim, x(2).trim, x(3).trim) )

    val transRDD=transdata.map(line => line.split("\t")).map(x => Row(x(0).trim,x(1).trim,x(2).trim,x(3).trim,x(4).trim ) )

    val custDF=sqlcontext.createDataFrame(custRDD,custSchema)
    val transDF=sqlcontext.createDataFrame(transRDD,transSchema)


    custDF.registerTempTable("customer")
    transDF.registerTempTable("transaction")

    sqlcontext.sql("select t.item_description,c.location,product_id,c.customer_id,t.sell_price from customer c, transaction t where c.customer_id=t.customer_id").registerTempTable("joinedtable")
    //•	Find the locations in which sale of each product is maximum
    sqlcontext.sql("select item_description, location, count(1) from joinedtable group by item_description,location order by 1,2")
    //•	Find the customer who has purchased max number of items
    sqlcontext.sql("select customer_id, count(item_description) as itemcount from joinedtable group by customer_id " )
    //•	Find the customer who has spent maximum money
    sqlcontext.sql("select customer_id, sum(sell_price) from joinedtable group by customer_id")
    //•	Find the product which has minimum sale in terms of money.
    sqlcontext.sql("select item_description, sum(sell_price) from joinedtable group by item_description")
    //•	Find the product which has minimum sale in terms of number of unit sold.
    sqlcontext.sql("select item_description, count(1) from joinedtable group by item_description").show()

  }

}
