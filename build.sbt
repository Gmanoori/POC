ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "POC"
  )

libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.18"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"
libraryDependencies += "org.postgresql"   %  "postgresql" % "42.7.3"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.4.1" % "provided"
