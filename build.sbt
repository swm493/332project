import sbt.Keys.libraryDependencies

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

    PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    ),

    assemblyMergeStrategy := {
      case PathList("META-INF", "io.netty.versions.properties") =>
        MergeStrategy.discard

      // 💡 Service Provider Configuration 파일들은 합쳐야 함
      case PathList("META-INF", "services", xs @ _*) =>
        MergeStrategy.concat // 파일 내용을 합침 (새 줄로 연결)

      // 💡 그 외 모든 META-INF 파일들은 버림 (잡음 제거)
      case PathList("META-INF", xs @ _*) =>
        MergeStrategy.discard

      case x =>
        (assemblyMergeStrategy).value(x)
    }
  )