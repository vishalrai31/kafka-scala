package exercise
import scala.collection.immutable.Seq

/**
  * Created by vrai on 3/24/2017.
  */
object ScalaStream {
  def main(args: Array[String]): Unit = {

    val str="abc123=abc#desf123"
    val arr=str.split("=")
    println(arr(0)+":::"+arr(1))
    val asd=Stream.continually(1 to 20).takeWhile(_.equals() != (40 to 60)).map(println )
    //val asdf=Stream.from(1).takeWhile(_ < 10).map(println)
   // println(asdf)

  }

}
