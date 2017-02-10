name := "kafka-pub-sub"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.10.1.0",
  "org.twitter4j" % "twitter4j-stream" % "3.0.3",
  //"org.json4s" %% "json4s-native" % "3.4.0",
  //"org.json4s" % "json4s-jackson_2.10" % "3.1.0",
  "org.json4s" %% "json4s-native" % "3.5.0",
  "net.liftweb" %% "lift-json" % "2.6+",
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-streaming" % "1.6.0",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0",
  "com.github.fge" % "json-schema-validator" % "2.2.6",
  "com.databricks" % "spark-csv_2.11" % "1.2.0"
)

    