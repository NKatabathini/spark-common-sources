name := "spark-common-sources"
organization := "com.claws.spark.sources"
scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.12.10" % "provided",
  "org.apache.spark" %% "spark-core" % "3.1.3"  % "provided",
  "org.apache.spark" %% "spark-sql" % "3.1.3"  % "provided",
  "org.apache.spark" %% "spark-hive" % "3.1.3"  % "provided",
  "org.apache.spark" %% "spark-avro" % "3.1.3"  % "provided"

)
