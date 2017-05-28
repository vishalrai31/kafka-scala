package transformation

import enumerations.{EnumerationAsStringUDT, SparkEnumeration}
import org.apache.spark.sql.types.{DataType, SQLUserDefinedType, StringType, UserDefinedType}
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by vrai on 5/27/2017.
  */
@SQLUserDefinedType(udt = classOf[ColumnConstraintUDT])
sealed trait ColumnConstraint

object ColumnConstraint extends SparkEnumeration[ColumnConstraint]{
  case object MANDATORY extends ColumnConstraint
  case object OPTIONAL extends ColumnConstraint
  case object NULLABLE extends ColumnConstraint

  def values:Seq[ColumnConstraint] =Seq(MANDATORY,OPTIONAL,NULLABLE)
}


class ColumnConstraintUDT extends EnumerationAsStringUDT[ColumnConstraint]{
  def getFromType(value:String):ColumnConstraint=ColumnConstraint.withName(value)
}

/*class ColumnConstraintUDT extends UserDefinedType[ColumnConstraint]{
  override def sqlType: DataType = StringType

  override def serialize(obj: Any): Any = UTF8String.fromString(obj.toString)

  override def deserialize(datum: Any): ColumnConstraint = getFromType(datum.asInstanceOf[UTF8String].toString)

  override def userClass: Class[ColumnConstraint] = null

  def getFromType(value:String):ColumnConstraint={
  ColumnConstraint.values.find(_.toString == value).getOrElse(
    throw new IllegalArgumentException(s"can not find enum with name$value")
  )

  }
}*/
