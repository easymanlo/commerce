/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 */

package com.atguigu.session

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

/**
  * 自定义累加器
  */
class SessionAggrStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  // 保存所有聚合数据
  private val aggrStatMap = mutable.HashMap[String, Int]()
  //判断累加器是否为空
  override def isZero: Boolean = {
    aggrStatMap.isEmpty
  }
  //把前一阶段的累加器重新的赋值给后一段
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new SessionAggrStatAccumulator
    aggrStatMap.synchronized{
      newAcc.aggrStatMap ++= this.aggrStatMap
    }
    newAcc
  }

  override def reset(): Unit = {
    aggrStatMap.clear()
  }

  override def add(v: String): Unit = {
    if (!this.aggrStatMap.contains(v))
      this. aggrStatMap += (v -> 0)
    this.aggrStatMap.update(v, aggrStatMap(v) + 1)
  }
   //把二个累加器进行合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
        //(0 /:(1 to 100)(_+_) foldLeft() 就是以0为初始值去遍历1到100，然后把各个字段进行相加
        //(0 /:(1 to 100)(_+_) foldLeft()  (0 /:(1 to 100){case (int 1 ,int2)=>int 1 +int 2} int2 对应 （1 to 100)这个序列中的每个元素
        //（1 to 100).foldLeft(0) 这个和上面的是相等的
        //acc.aggrStatMap.foldLeft(this.aggrStatMap)
      case acc:SessionAggrStatAccumulator => { acc.aggrStatMap.foldLeft(this.aggrStatMap)
        //map += ( k 如果之前有这个k则覆盖，无这个key则是追加
        //map.getOrElse(k, 0) 取map中的数据要是有key则取value 若是没有则取0 二个map相同key的value相加，然后更新前面的map

        (this.aggrStatMap /: acc.value){ case (map, (k,v)) => map += ( k -> (v + map.getOrElse(k, 0)) )}
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.aggrStatMap
  }
}
