name := "AppraisalSparkExecutor"

version := "1.0"

organization := "com.appraisal.spark.executor"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.1"
val appraisalsparkversion = "1.0"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.appraisal.spark" %% "appraisalspark" % appraisalsparkversion
)
