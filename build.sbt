import AssemblyKeys._

assemblySettings

name := "template-scala-parallel-similarproduct"

organization := "io.prediction"

parallelExecution in Test := false

test in assembly := {}

libraryDependencies ++= Seq(
  "io.prediction"    %% "core"          % "0.9.5" % "provided",
  "org.apache.spark" %% "spark-core"    % "1.5.1" % "provided",
  "org.apache.spark" %% "spark-mllib"   % "1.5.1" % "provided",
  "org.scalatest"    %% "scalatest"     % "2.2.1" % "test")
