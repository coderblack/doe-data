package akka.demo1

import akka.actor.{Actor, ActorSystem, Props}

class HelloActor extends Actor {
  override def receive: Receive = {
    case "haha" => {
      println(sender() + " haha")
      sender() ! "你好"
    }
    case "名字" => {
      println(sender() + " 名字")
      sender() ! "深似海"
    }
    case "好了" => {
      println(sender() + "好了")
      sender() ! "马上关闭"
      context.stop(self)
    }
  }
}


class HeiheiActor extends Actor {
  override def receive: Receive = {
    case "你好" => {
      println(sender() + " 你好")
      sender() ! "名字"
    }
    case "深似海" => {
      println(sender() + " 深似海")
      sender() ! "好了"
    }
    case "马上关闭" => {
      println(sender() + " 马上关闭")
      context.stop(self)
      context.system.terminate()
    }
  }
}

object Demo {

  private val actorSystem = ActorSystem("demo")

  def main(args: Array[String]): Unit = {
    val helloActorRef = actorSystem.actorOf(Props(classOf[HelloActor]), "haha")
    val heiheiActorRef = actorSystem.actorOf(Props(classOf[HeiheiActor]), "heihei")

    helloActorRef.tell("haha",heiheiActorRef)


  }


}
