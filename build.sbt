ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.7"
val grpcVersion = "1.64.0"

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)
libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
)

libraryDependencies ++= Seq(
  // 기존에 있던 것 (protobuf 메시지용)
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",

  // (추가) ScalaPB의 gRPC 헬퍼 (scalapb.grpc.*)
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,

  // (추가) gRPC-Java 런타임 (io.grpc.*)
  "io.grpc" % "grpc-netty" % grpcVersion,
  "io.grpc" % "grpc-protobuf" % grpcVersion,
  "io.grpc" % "grpc-stub" % grpcVersion
)

lazy val root = (project in file("."))
  .enablePlugins(ScalabPlugin) // <-- 이 줄을 추가해 주세요
  .settings(
    name := "my-distributed-sort",
    libraryDependencies ++= Seq(
      // gRPC와 Protobuf (ScalaPB)
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % "0.11.14",
      "io.grpc" % "grpc-netty" % "1.58.0", // gRPC 서버/클라이언트 런타임
      "com.google.protobuf" % "protobuf-java" % "3.25.1", // Protobuf 런타임

      // 테스트
      "org.scalatest" %% "scalatest" % "3.2.17" % Test
    ),
    // ScalaPB 플러그인 활성화 및 gRPC 코드 생성 설정
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    )
  )