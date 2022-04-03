name := "FreqSeqPatterns"

version := "1.0"

scalaVersion := "2.11.12"

//resolvers += "bintray/meetup" at "http://dl.bintray.com/meetup/maven"
//resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.4.5" % "provided",
  "org.spire-math" %% "archery" % "0.4.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test//,
  //"org.clustering4ever" %% "clustering4ever" % "0.9.8"

)

excludeFilter in unmanagedSources := "mean_shift_lsh_1.2.1.scala" || "mean_shift_lsh_1.0.5.scala" ||
"BisectingKMeans.scala"||"BisectingKMeansModel.scala"||"DistanceMeasure.scala"

test in assembly := {}