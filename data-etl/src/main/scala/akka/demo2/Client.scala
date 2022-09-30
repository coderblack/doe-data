package akka.demo2

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory


class ClientActor(host:String,port:Int) extends Actor{

  var serverActorRef: ActorSelection = _ // 服务端的代理对象

  override def preStart(): Unit = {
    // akka.tcp://Server@127.0.0.1:8088
    serverActorRef = context.actorSelection(s"akka://Server@${host}:${port}/user/Miao~miao")
  }

  override def receive: Receive = {
    case "start" =>
      serverActorRef ! Message("握手")
    case Message(msg:String)=>
      println(s"客户端收到$msg")
      serverActorRef ! Message("你好")
    case _ =>
      serverActorRef ! Message("听不懂")
  }


}


object Client {

  def main(args: Array[String]): Unit = {


    //指定客户端的IP和端口
    val host = "127.0.0.1"
    val port  = 8089

    //指定服务端的IP和端口
    val serverHost = "127.0.0.1"
    val serverPort = 8088

    /**
     * 使用ConfigFactory的parseString方法解析字符串,指定客户端IP和端口
     */
    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.artery.enable="on"
         |akka.remote.artery.canonical.hostname=$host
         |akka.remote.artery.canonical.port=$port
         |akka.actor.allow-java-serialization=true
         """.stripMargin)

    /**
     * 将config对象传递给ActorSystem并起名为"Server"，为了是创建客户端工厂对象(clientActorSystem)。
     */
    val clientActorSystem = ActorSystem("client", config)

    // 创建dispatch | mailbox
    val clientActorRef = clientActorSystem.actorOf(Props(new ClientActor(serverHost, serverPort)), "Client")
    clientActorRef ! "start" // 自己给自己发送了一条消息 到自己的mailbox => receive

  }
}
