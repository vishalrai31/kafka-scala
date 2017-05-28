import org.apache.spark.sql.UserDefinedFunction
import org.apache.spark.sql.types.{DataType, StringType, UserDefinedType}
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by vrai on 5/27/2017.
  */
package object enumerations {

trait EnumerationAsStringUDT[T] extends UserDefinedType[T]{
  override def sqlType: DataType = StringType

  override def serialize(obj: Any): Any = UTF8String.fromString(obj.toString)
  override def userClass: Class[T] = null
  override def deserialize(datum: Any): T = getFromType(datum.asInstanceOf[UTF8String].toString)

  def getFromType(value:String):T

}

  trait SparkEnumeration[T] extends Serializable{
    def values:Seq[T]
    def withName(name:String):T=values.find(_.toString == name).getOrElse(throw new IllegalArgumentException(s"can not find enum for name $name"))
  }

}
