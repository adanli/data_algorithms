name := "data_algorithms"

version := "1.0"

scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-mllib" % "3.5.0",
  "org.apache.spark" %% "spark-streaming" % "3.5.0",
  "org.apache.spark" %% "spark-hive" % "3.5.0",
  "org.apache.hadoop" % "hadoop-client" % "3.3.4",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.mysql" % "mysql-connector-j" % "8.0.33",
)

//mainClass in (Compile, packageBin) := Some("ml.DecisionTreeFeature")