name := "RelatedHashtags"

version := "0.1"

libraryDependencies  ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.1",
)

scalaVersion := "2.12.10"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
