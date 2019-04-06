name := "Flink-Kafka"

version := "0.1"

scalaVersion := "2.11.8"

lazy val versions = new {
  val hadoopVersion = "2.7.7"
  val flinkVersion = "1.7.1"
}

libraryDependencies ++= {
  Seq(
    "org.apache.flink"      %%  "flink-scala"                 % versions.flinkVersion   % "provided",
    "org.apache.flink"      %%  "flink-streaming-scala"       % versions.flinkVersion   % "provided",
    "org.apache.flink"      %%  "flink-connector-kafka-0.10"  % versions.flinkVersion   % "compile",
    "com.couchbase.client"   %  "java-client"                 % "2.7.1",
    "io.spray"              %%  "spray-json"                  % "1.3.5"
  )   
}
