ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "POC",
    // Copy JAR to spark-jars directory after packaging
    Compile / packageBin / artifactPath := baseDirectory.value / "spark-jars" / s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"
  )

libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.18"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "org.postgresql"   %  "postgresql" % "42.7.3"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.5.0" % "provided"