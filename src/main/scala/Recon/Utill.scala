package Recon


import org.apache.spark.sql.types.{StructField, StructType}

/**
  * Created by vrai on 2/11/2017.
  */
object Utill {

  def inferringSchemaFromFile(dfSchema:StructType)={

    val inferredSchema: List[ColumnDetail] =dfSchema.fields.map{
      structField =>
        returnColumnFromStructField(structField)
    }.toList
    inferredSchema
  }
  def returnColumnFromStructField(structField:StructField)={
    val dataType: String =structField.dataType.toString.toUpperCase match {
      case TypeConstants.double => TypeConstants.amount
      case x => x.toString
    }
    ColumnDetail(structField.name,dataType)
  }

}

object TypeConstants{
  final val amount="AMOUNT"
  final val int="INT"
  final val double="DOUBLE"
  final val long="LONG"

}

case class ColumnDetail(name:String, ctype:String)
