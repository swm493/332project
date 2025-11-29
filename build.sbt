import scala.collection.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.7"

val grpcVersion = "1.64.0"
val scalapbVersion = "0.11.11"

lazy val root = (project in file("."))
  .settings(
    name := "332project",

    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    ),

    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapbVersion,
      "io.grpc" % "grpc-netty" % grpcVersion,
      "io.grpc" % "grpc-protobuf" % grpcVersion,
      "io.grpc" % "grpc-stub" % grpcVersion
    ),

    assemblyMergeStrategy := {
      case PathList("META-INF", "io.netty.versions.properties") =>
        MergeStrategy.discard
      case PathList("META-INF", "services", xs @ _*) =>
        MergeStrategy.concat
      case PathList("META-INF", xs @ _*) =>
        MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )