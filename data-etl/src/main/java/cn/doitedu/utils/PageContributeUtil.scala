package cn.doitedu.utils

import scala.collection.mutable.ListBuffer

case class TreeNode(pageId: String, children: ListBuffer[TreeNode])

object PageContributeUtil {

  def main(args: Array[String]): Unit = {

    val recordsStr = "a,1|b,a|d,b|e,b|c,a|a,c|x,a"
    val records = recordsStr.split("\\|")

    var node: TreeNode = null
    // 遍历每一次页面访问记录，形成树节点，并挂载到树的正确位置上去
    for (record <- records) {
      val splits = record.split(",")
      val pageId = splits(0)
      val referPageId = splits(1)

      if (node == null) {
        node = TreeNode(pageId, ListBuffer.empty)
      } else {
        findAndAppend(node, pageId, referPageId)
      }
    }

    // 打印树
    println(node)

    println("------------------------")

    // 打印每个节点的总贡献量
    val lst1 = ListBuffer.empty[(String, Int)]
    calcWholeContributePv(node,lst1)
    println(lst1)

    println("------------------------")

    // 打印每个节点的总贡献量
    val lst2 = ListBuffer.empty[(String, Int)]
    calcDirectContributePv(node,lst2)
    println(lst2)



  }

  // 将一条页面访问记录，挂载到一棵树上的计算逻辑
  def findAndAppend(node: TreeNode, pageId: String, referPageId: String): Boolean = {

    // 先去节点的所有子节点中去寻找目标挂载点
    for (childNode <- node.children.reverse) {
      val find = findAndAppend(childNode, pageId, referPageId)
      // 如果在某一个子节点上找到了目标挂载点，则返回
      if (find) return true
    }

    // 如果在上面的过程中没有返回，说明整个for循环遍历的每一个子节点上都没找到目标挂载点
    // 则判断我自己是不是目标挂载点

    if (node.pageId.equals(referPageId)) {
      node.children += TreeNode(pageId, ListBuffer.empty)
      true
    } else {
      false
    }
  }


  // 计算一棵树上每个节点（页面）的总贡献量
  def calcWholeContributePv(node:TreeNode, lst:ListBuffer[(String,Int)]):Int = {

    var contributePv = 0

    // 本节点的总贡献量  =  本节点子节点个数  +  每个子节点的总贡献量

    // 先加上子节点个数
    contributePv += node.children.size

    for (childNode <- node.children) {
      // 然后去加  每个子节点的总贡献量
      val childContributePv = calcWholeContributePv(childNode, lst)
      contributePv += childContributePv
    }

    // 把本节点算出来的总贡献量，放入结果list中去
    lst += ((node.pageId,contributePv))

    contributePv
  }


  // 计算一棵树上每个节点（页面）的直接贡献量
  def calcDirectContributePv(node:TreeNode, lst:ListBuffer[(String,Int)]):Unit = {

    // 本节点的直接贡献量  =  本节点子节点个数
    // 先加上子节点个数
    //var contributePv = node.children.size

    for (childNode <- node.children) {
      // 然后去加  每个子节点的总贡献量
      calcDirectContributePv(childNode, lst)
    }

    // 把本节点算出来的总贡献量，放入结果list中去
    lst += ((node.pageId,node.children.size))
  }





}
