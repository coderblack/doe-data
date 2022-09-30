package akka.demo2

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory


class ServerActor extends Actor{

  override def receive: Receive = {

    case Message(msg)=>
      println("服务端收到消息：" + msg)
      sender() ! Message("服务端来了")
    case _ =>
      println("服务端没听懂")
      sender() ! "没听懂"
  }
}

object Server {
  def main(args: Array[String]): Unit = {
    //定义服务端的ip和端口
    val host = "127.0.0.1"
    val port = 8088
    /**
     * 使用ConfigFactory的parseString方法解析字符串,指定服务端IP和端口
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
     * 将config对象传递给ActorSystem并起名为"Server"，为了是创建服务端工厂对象(ServerActorSystem)。
     */
    val ServerActorSystem = ActorSystem("Server", config)
    /**
     * 通过工厂对象创建服务端的ActorRef
     */
    val serverActorRef = ServerActorSystem.actorOf(Props[ServerActor], "Miao~miao")

  }
}
