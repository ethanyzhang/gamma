name := "GammaSpark"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.0",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.0",
  "org.scalanlp" %% "breeze-natives" % "0.12"
)

