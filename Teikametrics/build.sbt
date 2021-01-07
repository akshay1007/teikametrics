name := "Teikametrics"

version := "1.0"

scalaVersion :=  "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0-preview2",
  "org.apache.spark" %% "spark-sql" % "3.0.0-preview2"
)

libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.2.2" % "test"



