package exercise



import java.io._
import java.util.Scanner
import java.util.zip.{ZipEntry, ZipFile, ZipInputStream}

import org.apache.commons.io.IOUtils
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.immutable.Seq

/**
  * Created by vrai on 3/25/2017.
  */
object SparkZipFileReader {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("ziptest").setMaster("local")
    val sc: SparkContext =new SparkContext(sparkConf)
    val sqlContext: SQLContext =new SQLContext(sc)

   /* val fis = new FileInputStream("D:\\spark\\files\\files.zip")
    val zis = new ZipInputStream(fis)
    Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach{ file =>
      val fout = new FileOutputStream("D:\\spark\\files\\"+file.getName)
      val buffer = new Array[Byte](1024)
      Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(fout.write(buffer, 0, _))
    }*/



    val files: RDD[(String, PortableDataStream)] =sc.binaryFiles("D:\\spark\\files\\files.zip")
    files.map{
      case (filename, zipcontent) =>
        val zipInputStream: ZipInputStream = new ZipInputStream(zipcontent.open())
        //val sd=Stream.continually(zipInputStream.getNextEntry).takeWhile(x => x!= null).filter(x => x.getName.startsWith("emptyttt") ).print()
        Stream.continually(zipInputStream.getNextEntry).takeWhile(x => x!=null).filter(x => x!= Stream.empty).map{
            value =>
            if(value.getName.startsWith("env")){
              val inputStream=IOUtils.toBufferedInputStream(zipInputStream)
              print(scala.io.Source.fromInputStream(inputStream).getLines().mkString("####"))
            }
            else{
              val inputStream=IOUtils.toBufferedInputStream(zipInputStream)
              print(scala.io.Source.fromInputStream(inputStream).getLines().mkString("\n"))
            }
        }
         }.collect()



      /* 1...
          val out = new ByteArrayOutputStream();
          IOUtils.copy(zipInputStream, out);
          val inputStream = new ByteArrayInputStream(out.toByteArray())
        2...
          val sc = new Scanner(zipInputStream);
          while(sc.hasNext){
          println(sc.nextLine())


         */





  }

}
