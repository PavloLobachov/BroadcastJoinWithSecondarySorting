package com.spark.task.partititoner

import org.apache.spark.Partitioner

/**
  * Created by pavlolobachov on 3/28/18.
  */
class RoomBasedPartitioner[K, S](override val numPartitions: Int) extends Partitioner {
  require(numPartitions >= 0, s"Number of partitions cannot be negative but found $numPartitions.")

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(K, S)]
    Math.abs(k.hashCode) % numPartitions
  }

}
