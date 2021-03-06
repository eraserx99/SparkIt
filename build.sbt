name := "SparkIt"

version := "0.1"

scalaVersion := "2.11.8"

version := "1.0"

libraryDependencies ++= {
  val sparkVer = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer,
    "mysql" % "mysql-connector-java" % "6.0.6",
    "org.scalatest" %% "scalatest" % "3.0.5" % Test
  )
}