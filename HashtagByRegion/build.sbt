name := "HashtagByRegion"

version := "0.1"

libraryDependencies  ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "com.amazonaws" % "aws-java-sdk" % "1.11.948",
  "org.apache.hadoop" % "hadoop-aws" % "2.8.5"
)

scalaVersion := "2.12.13"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
