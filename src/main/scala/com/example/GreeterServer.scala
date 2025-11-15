package com.example

import java.net.InetSocketAddress // 0.0.0.0 바인딩을 위해 import
import io.grpc.ServerBuilder

// 자동 생성된 코드 Import
import com.example.grpc.greeter.GreeterGrpc
import com.example.grpc.greeter.HelloReply
import com.example.grpc.greeter.HelloRequest

// Scala의 비동기 처리를 위한 Future와 ExecutionContext
import scala.concurrent.{ExecutionContext, Future}

/**
 * GreeterGrpc.Greeter 트레이트(trait)를 구현하여 실제 서비스 로직 작성
 * (이 클래스가 GreeterClient.scala에 있으면 안 됩니다)
 */
class GreeterImpl(implicit ec: ExecutionContext) extends GreeterGrpc.Greeter {

  override def sayHello(request: HelloRequest): Future[HelloReply] = {
    // 워커 노드로부터 받은 이름으로 응답
    val reply = HelloReply(message = s"Hello from Master, ${request.name}!")
    Future.successful(reply)
  }
}

/**
 * 서버를 시작(Bootstrap)하는 main 객체
 */
object GreeterServer {

  def main(args: Array[String]): Unit = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val service = new GreeterImpl
    val serviceDefinition = GreeterGrpc.bindService(service, ec)

    val port = 50051

    val server = ServerBuilder
      .forPort(port) // 이 코드가 GreeterClient.scala에 있으면 안 됩니다
      .addService(serviceDefinition)
      .build()

    server.start()
    println(s"gRPC 서버가 0.0.0.0:${port} 에서 시작되었습니다.")
    println("워커 노드의 접속을 대기합니다...")

    server.awaitTermination()
  }
}