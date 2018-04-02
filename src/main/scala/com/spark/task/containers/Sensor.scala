package com.spark.task.containers

import java.time.LocalDateTime

import com.spark.task.util.Util

import scala.util.Try

/**
  * Created by pavlolobachov on 3/24/18.
  */

case class SensorDevice(sensorId: String, channelId: Int, channelType: String, locationId: String)

case class SensorData(sensorId: String, channelId: Int, timestamp: LocalDateTime, var value: Double)

case class RecordId(sensorId: String, channelId: Int)

case class TempSensorData(sensorId: String, channelId: Int, channelType: String, locationId: String, timestamp: LocalDateTime, var value: Double)

case class SensorReport(var timeSlotStart: LocalDateTime = null,
                        var location: String = "",
                        var tempMin: Double = 0, var tempMax: Double = 0, var tempAvg: Double = 0,
                        var tempCnt: Int = 0, var presence: Boolean = false, var presenceCnt: Int = 0) {

  override def toString: String = {
    if (tempCnt == 0) {
      s"$timeSlotStart,$location,,,,$tempCnt,$presence,$presenceCnt"
    } else {
      s"$timeSlotStart,$location,$tempMin,$tempMax,$tempAvg,$tempCnt,$presence,$presenceCnt"
    }
  }
}


object Sensor {

  def strToSensorReport(csvData: String): SensorReport = {
    val data = csvData.split(",").map(elem => elem.trim)
    SensorReport(
      timeSlotStart = Util.stringToLocalDateTime(data(0)),
      location = data(1),
      tempMin = Try(data(2).toDouble).getOrElse(0L),
      tempMax = Try(data(3).toDouble).getOrElse(0L),
      tempAvg = Try(data(4).toDouble).getOrElse(0L),
      tempCnt = data(5).toInt,
      presence = data(6).toBoolean,
      presenceCnt = data(7).toInt)
  }

  def strToSensorDevice(csvData: String): SensorDevice = {
    val data = csvData.split(",").map(elem => elem.trim)
    SensorDevice(data(0), data(1).toInt, data(2), data(3))
  }

  def strToSensorData(csvData: String): SensorData = {
    val data = csvData.split(",").map(elem => elem.trim)
    SensorData(data(0), data(1).toInt, Util.stringToLocalDateTime(data(2)), Try(data(3).toDouble).getOrElse(0L))
  }

}
