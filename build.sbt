name := "interview-de-f5026e4c"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5"
