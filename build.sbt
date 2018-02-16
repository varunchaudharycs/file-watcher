name := "file-watcher"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.1",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0",
  "org.apache.commons" % "commons-io" % "1.3.2",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0"
)