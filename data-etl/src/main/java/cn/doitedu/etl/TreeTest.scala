package cn.doitedu.etl

import scala.collection.mutable.ListBuffer

case class Node(val pageId: String, val children: ListBuffer[Node])

object TreeTest {
  def main(args: Array[String]): Unit = {

    /*
       a -- |
            |---b---|--- d
                    |--- e
            |---c---|
                    |---a--|
                           |---x

     */
    val str = "a,1|b,a|d,b|e,b|c,a|a,c|x,a"

    val pairs = str.split("\\|")
    var node: Node = null
    for (pair <- pairs) {
      val split = pair.split(",")
      val pageId = split(0)
      val refId = split(1)
      if (node == null) {
        node = Node(pageId, ListBuffer.empty[Node])
      } else {
        findAndAppend(node, pageId, refId)
      }
    }

    val tmp = ListBuffer.empty[(String, Int)]
    calcNodeContribute(node, tmp)
    println(tmp)

    val tuples = calcNodeContribute2(node)
    println("---------")
    println(tuples)


  }

  // 挂载节点到树
  def findAndAppend(node: Node, pageId: String, refId: String): Boolean = {
    for (childNode <- node.children.reverse) { // 反转遍历，是为了先找右子树
      val flag = findAndAppend(childNode, pageId, refId)
      if (flag) return flag
    }

    if (node.pageId.equals(refId)) {
      node.children += Node(pageId, ListBuffer.empty[Node])
      true
    } else {
      false
    }


  }


  // 计算总贡献量
  def calcNodeContribute(node: Node, tmp: ListBuffer[(String, Int)]): Int = {
    var pv = 0
    pv += node.children.size
    for (cnode <- node.children) {
      pv += calcNodeContribute(cnode, tmp)
    }
    tmp += ((node.pageId, pv))
    pv
  }


  // 计算直接贡献量
  def calcNodeContribute2(node: Node): ListBuffer[(String, Int)] = {
    val lst = ListBuffer.empty[(String, Int)]
    for (cnode <- node.children) {
      lst ++= calcNodeContribute2(cnode)
    }
    lst += ((node.pageId, node.children.size))

  }

}
