package Recon

import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jackson.JsonLoader
import com.github.fge.jsonschema.core.report.ProcessingReport
import com.github.fge.jsonschema.main.{JsonSchemaFactory, JsonValidator}
import org.json4s.JsonAST.JObject



/**
  * Created by vrai on 1/25/2017.
  */
object JsonValidator {

  def main(args :Array[String]): Unit = {

   /* val schemaJson: String =
      """{
  'description': 'A person',
  'type': 'object',
  'properties':
  {
    'name': {'type':'string'},
    'hobbies': {
    'type': 'array',
    'items': {'type':'string'}
  }
  }
}""";*/

    val schemaJson: String= """{
      "$schema": "http://json-schema.org/draft-03/schema",
      "type": ["object","string"],
      "properties": {
        "geo": {"$ref": "http://jsonschema.net/examples/C.json"},
        "address": {
        "type": "object",
        "properties": {
        "city": {
        "type": "string"
      },
        "streetAddress": {
        "type": "string"
      }
      }
      },
        "age":{
        "type": "number"
      }
      }
    }"""


/*
    val jsonvalue: String =
      """ 'name': 'James',
  'hobbies': ['.NET', 'Blogging', 'Reading', 'Xbox', 'LOLCATS'] """;*/

    val jsonvalue: String= """{
      "address":{
        "streetAddress": "21 2nd Street",
        "city":"New York"
      },
      "phoneNumber":
      [
      {
        "type":"home",
        "number":"212 555-1234"
      }
      ],
      "age" : 123
    }"""


    val data: JsonNode = JsonLoader.fromString(jsonvalue)
    val schema: JsonNode = JsonLoader.fromString(schemaJson)
    val factory: JsonSchemaFactory = JsonSchemaFactory.byDefault()
    val validator: JsonValidator = factory.getValidator()
    val report: ProcessingReport = validator.validate(schema, data)
    println("report>>>>>"+report.isSuccess())


    /* val json= """{"hello":"world"}"""
  val jsonSchema = """{
                     |  "title":"hello world schema",
                     |  "type":"object",
                     |  "properties":{
                     |    "hello": {
                     |      "type": "string"
                     |    }
                     |  },
                     |  "required":["hello"]
                     |}""".stripMargin

  val schema: JsonNode = asJsonNode(parse(jsonSchema))
  val instance: JsonNode = asJsonNode(parse(json))

  val validator = JsonSchemaFactory.byDefault().getValidator

  val processingReport = validator.validate(schema, instance)

  if (processingReport.isSuccess) {
    println("JSON Schema validation was successful")
  } else {
      println("fail..")

  }*/


  }
}
