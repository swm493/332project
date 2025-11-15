package com.example

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import com.example.grpc.greeter.{GreeterGrpc, HelloReply, HelloRequest}
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * 클라이언트를 시작하는 main 객체
 */
object GreeterClient {

  def main(args: Array[String]): Unit = {
    implicit val ec: ExecutionContext = ExecutionContext.global

    val serverIp = "141.223.16.227"
    val serverPort = 50051

    val channel: ManagedChannel = ManagedChannelBuilder
      .forAddress(serverIp, serverPort)
      .usePlaintext()
      .build()

    val stub = GreeterGrpc.stub(channel)
    val request = HelloRequest(name = "Worker Node (vm01)")

    println(s"마스터 서버(${serverIp}:${serverPort})로 요청 전송: ${request.name}")

    val futureReply = stub.sayHello(request)

    futureReply.onComplete {
      case Success(reply) =>
        println(s"서버로부터 응답 받음: ${reply.message}")
      case Failure(e) =>
        println(s"에러 발생 (방화벽 확인): ${e.getMessage}")
    }

    try {
      Await.result(futureReply, 5.seconds)
    } catch {
      case e: Exception => //
    } finally {
      channel.shutdownNow()
      channel.awaitTermination(5, SECONDS)
      println("클라이언트 종료.")
    }
  }
}