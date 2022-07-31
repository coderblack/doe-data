package cn.doitedu.utils

object EventAttrUtil {

  /**
   * 首次触点归因算法
   * eventSeq ==>  ["e3","e2","e3","e2","e1"]
   * eventSeq ==>  ["e1"]
   */
  def firstTouchAttr(eventSeq:Array[String]):(String,Double) ={
    if(eventSeq.length > 1) {
      (eventSeq(0),1.0)
    }else{
      (null,0.0)
    }
  }



  /**
   * 末次触点归因算法
   * eventSeq ==>  ["e3","e2","e3","e2","e1"]
   * eventSeq ==>  ["e1"]
   */
  def lastTouchAttr(eventSeq:Array[String]):(String,Double) ={
    if(eventSeq.length > 1) {
      (eventSeq(eventSeq.length-2),1.0)  // [e3,e1]
    }else{
      (null,0.0)
    }
  }



  /**
   * 线性归因算法
   * eventSeq ==>  ["e3","e2","e3","e2","e1"]
   */
  def linearAttr(eventSeq:Array[String]):List[(String,Double)] ={
    if(eventSeq.length > 1) {

      // ["e3","e2","e3","e2","e1"] => [(e3,0.2),(e2,0.2),(e3,0.2),(e2,0.2),(e1,0.2)]
      val res: Map[String, Double] = eventSeq
        .reverse
        .tail
        .reverse
        .map(e => (e, 1.0 / (eventSeq.length - 1)))
        .groupBy(tp => tp._1)
        .mapValues(arr => arr.map(tp => tp._2).sum)

      res.toList

    }else{
      List((null,0.0))
    }
  }

}
