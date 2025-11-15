ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.7"

val grpcVersion = "1.64.0"
val scalapbVersion = "0.11.17"

lazy val root = (project in file("."))
  .settings(
    name := "332project",

    Compile / PB.protoSources := Seq(
      (Compile / sourceDirectory).value / "protobuf"
    ),

    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    ),

    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapbVersion,
      "io.grpc" % "grpc-netty" % grpcVersion,  // shaded 대신 일반 버전
      "io.grpc" % "grpc-protobuf" % grpcVersion,
      "io.grpc" % "grpc-stub" % grpcVersion
    ),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat  // 중요!
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )