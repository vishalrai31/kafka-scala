package exercise

/**
  * Created by vrai on 3/26/2017.
  */
trait A{ def string ="main string"}
trait B extends A { override def string ="B String" + super.string }
trait C extends B { override def string ="C String" + super.string}
class Multipletrait extends B with C{

}
object Multipletrait {
  def main(args: Array[String]): Unit = {
    println(new Multipletrait().string)
  }
}
