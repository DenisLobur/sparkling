name := "sparkling"

version := "0.1"

scalaVersion := "2.11.11"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= List(
  "org.scalafx" %% "scalafx" % "8.0.144-R12",
  "org.apache.spark" % "spark-core_2.11" % "2.2.0"
)