lazy val root = (project in file("."))
  .settings(SparkSubmit.settings: _*)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

name := "MongoDB-to-S3"

version := "1.3.0"

organization := "com.facil"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
    // Spark dependency
    "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
    // Third-party libraries
    "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.3",
    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
    "org.apache.hadoop" % "hadoop-aws" % "2.7.3"
)
