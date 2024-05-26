ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "OlympusPanoptes"
  )

version := "0.1"

scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.apache.spark" %% "spark-mllib" % "3.3.0",
  "org.apache.spark" %% "spark-streaming" % "3.3.0",
  "org.locationtech.spatial4j" % "spatial4j" % "0.7"
)
