name := "SparkApp"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.0",
  "org.postgresql" % "postgresql" % "42.6.0"
)

ThisBuild / fork := true

ThisBuild / javaOptions ++= Seq(
  "-Dlog4j.configuration=file:log4j.properties",
  "-Xms1g",
  "-Xmx2g"
)
