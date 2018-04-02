package com.spark.task

import java.time.LocalDateTime

import com.spark.task.argparse.Parser
import com.spark.task.configuration.ParserConfiguration
import com.spark.task.containers._
import com.spark.task.partititoner.RoomBasedPartitioner
import com.spark.task.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{Map, mutable}

/**
  * Created by pavlolobachov on 3/24/18.
  */
object ApplicationReportReprocessing {
  val ProcessMode: String = "process"
  val ReprocessMode: String = "reprocess"
  val DayToMilliseconds: Int = 86400000
  val HourToMilliseconds: Int = 3600000
  val MinuteToMilliseconds: Int = 60000
  val FifteenMinutesWindow: Int = 15
  val HourWindow: Int = 60
  private val logger: Logger = LoggerFactory.getLogger(ApplicationReportReprocessing.getClass.getName)
  implicit val ordering: Ordering[(String, Long)] = Ordering.Tuple2

  def main(args: Array[String]): Unit = {

    Parser.getParser.parse(args, ParserConfiguration()) match {
      case Some(cliArgs) => {
        val mode: String = cliArgs.mode
        val from: Long = cliArgs.from
        val until: Long = cliArgs.until

        if (from != 0 && until != 0) {
          if (until < from) {
            logger.error("[until-date] should be greater than [from-date]")
            sys.exit(1)
          }

        }

        val sensorDeviceFile = cliArgs.sensorDevicesDataFile
        val sensorDataFile = cliArgs.sensorDataFile
        val sensorReportFile = cliArgs.sensorReportFile
        val targetDirectory = cliArgs.targetDirectory
        val numOfPartitions = cliArgs.numOfRooms

        val conf = new SparkConf().setAppName("TestSparkApplication")
        val sc = new SparkContext(conf)
        val master = conf.get("spark.master")

        if (mode == ReprocessMode) {
          Util.validatePath(sensorReportFile, master, sc)
          reprocessReport(sc, numOfPartitions, sensorReportFile, targetDirectory, from, until)
        } else {
          Util.validatePath(sensorDataFile, master, sc)
          Util.validatePath(sensorDeviceFile, master, sc)
          processReports(sc, numOfPartitions, sensorDeviceFile, sensorDataFile, targetDirectory, from, until)
        }
        sc.stop()
      }
      case None =>
      // arguments are bad, error message will have been displayed
    }
  }


  private[task] def reprocessReport(sc: SparkContext, numOfRooms: Int, reportFile: String, targetDirectory: String, from: Long = 0L, until: Long = 0L): Unit = {
    val filter = from != 0 && until != 0

    val sensorReportRDD: RDD[((String, Long), SensorReport)] = sc.textFile(reportFile)
      .map(r => Sensor.strToSensorReport(r))
      .keyBy((sr: SensorReport) => (sr.location, Util.localDateTimeToMills(sr.timeSlotStart)))

    val filteredSensorReportRDD = if (filter) {
      sensorReportRDD.filter { case ((_, sk), v) => sk >= from && sk <= (until + DayToMilliseconds) }
    } else {
      sensorReportRDD
    }

    val roomSortedRDD: RDD[((String, Long), SensorReport)] = filteredSensorReportRDD.repartitionAndSortWithinPartitions(
      new RoomBasedPartitioner[String, Long](numOfRooms)
    )

    val oneHourGroupedRDD = groupAndCalculateStats[SensorReport](roomSortedRDD, HourWindow, (sr: SensorReport) => Util.localDateTimeToMills(sr.timeSlotStart), recalculateStats)

    if (filter) {
      addEmptyRecords(oneHourGroupedRDD.values, HourWindow, from, until + DayToMilliseconds).coalesce(1).saveAsTextFile(s"$targetDirectory/1H_Report")
    } else {
      oneHourGroupedRDD.values.coalesce(1).saveAsTextFile(s"$targetDirectory/1H_Report")
    }

  }

  private[task] def processReports(sc: SparkContext, numOfRooms: Int, sensorDeviceFile: String, sensorDataFile: String, targetDirectory: String, from: Long = 0L, until: Long = 0L): Unit = {
    val filter = from != 0 && until != 0
    val localSensorDevice: Map[RecordId, SensorDevice] = sc.textFile(sensorDeviceFile)
      .map(r => Sensor.strToSensorDevice(r))
      .filter(r => r.channelType != "battery")
      .keyBy(sd => RecordId(sd.sensorId, sd.channelId))
      .collectAsMap()

    sc.broadcast(localSensorDevice)

    val sensorDataRDD: RDD[(RecordId, SensorData)] = sc.textFile(sensorDataFile)
      .map(r => Sensor.strToSensorData(r))
      .keyBy(sd => RecordId(sd.sensorId, sd.channelId))

    val allDevicesRDD: RDD[((String, Long), TempSensorData)] = sensorDataRDD.mapPartitions(iter => {
      iter.flatMap {
        case (k, v1) => localSensorDevice.get(k) match {
          case None => Seq.empty[((String, Long), TempSensorData)]
          case Some(v2) => Seq(
            (
              (v2.locationId, Util.localDateTimeToMills(v1.timestamp)),
              TempSensorData(v1.sensorId, v1.channelId, v2.channelType, v2.locationId, v1.timestamp, v1.value))
          )
        }
      }
    })

    val filteredDevicesRDD = if (filter) {
      allDevicesRDD.filter { case ((_, sk), _) => sk >= from && sk <= (until + DayToMilliseconds) }
    } else {
      allDevicesRDD
    }

    val roomSortedRDD: RDD[((String, Long), TempSensorData)] = filteredDevicesRDD.repartitionAndSortWithinPartitions(
      new RoomBasedPartitioner[String, Long](numOfRooms)
    )

    val fifteenMinutesGroupedRDD = groupAndCalculateStats[TempSensorData](roomSortedRDD, FifteenMinutesWindow, (tsd: TempSensorData) => Util.localDateTimeToMills(tsd.timestamp), calculateStats)

    fifteenMinutesGroupedRDD.cache()

    val oneHourGroupedRDD = groupAndCalculateStats[SensorReport](fifteenMinutesGroupedRDD, HourWindow, (sr: SensorReport) => Util.localDateTimeToMills(sr.timeSlotStart), recalculateStats)

    if (filter) {
      addEmptyRecords(fifteenMinutesGroupedRDD.values, FifteenMinutesWindow, from, until + DayToMilliseconds)
        .coalesce(1).saveAsTextFile(s"$targetDirectory/15m_Report")
      addEmptyRecords(oneHourGroupedRDD.values, HourWindow, from, until + DayToMilliseconds)
        .coalesce(1).saveAsTextFile(s"$targetDirectory/1H_Report")
    } else {
      fifteenMinutesGroupedRDD.values.coalesce(1).saveAsTextFile(s"$targetDirectory/15m_Report")
      oneHourGroupedRDD.values.coalesce(1).saveAsTextFile(s"$targetDirectory/1H_Report")
    }
  }

  private[task] def addEmptyRecords(reportRDD: RDD[SensorReport], windowSizeInMinutes: Long, from: Long, until: Long): RDD[SensorReport] = {
    val timeSlice = until - from
    val numOfRecords = timeSlice / MinuteToMilliseconds / windowSizeInMinutes

    reportRDD.mapPartitions(iter => {
      val partitionData = iter.toList
      val SensorReport(timeSlot, locationId, _, _, _, _, _, _) = partitionData.last
      val recordDiff = numOfRecords - partitionData.size

      var previousTimeSlot = Util.localDateTimeToMills(timeSlot)
      val dummyRecords = (1L to recordDiff).map(_ => {
        previousTimeSlot += (windowSizeInMinutes * MinuteToMilliseconds)
        val dummyRecord = SensorReport(Util.millsToLocalDateTime(previousTimeSlot), locationId)
        dummyRecord
      }).toList

      (partitionData ::: dummyRecords).iterator
    })
  }

  private[task] def groupAndCalculateStats[V](roomSortedRDD: RDD[((String, Long), V)],
                                              windowInterval: Int,
                                              timeFunc: V => Long,
                                              calcFunc: (String, LocalDateTime, List[V]) => SensorReport): RDD[((String, Long), SensorReport)] = {
    roomSortedRDD.mapPartitions(iter => {
      val groupedByTime = groupByTimeRange[V](iter, windowInterval, timeFunc)
      groupedByTime.map { case (key@(pk, sk), value) => (key, calcFunc(pk, Util.millsToLocalDateTime(sk), value)) }
    }, preservesPartitioning = true)
  }

  private[task] def groupByTimeRange[V](timeSeries: Iterator[((String, Long), V)],
                                        timeWindow: Int,
                                        timeFunc: V => Long): Iterator[((String, Long), List[V])] = {
    val windowMillis = MinuteToMilliseconds * timeWindow
    var first = true
    var startRow = 0L
    var endRow = 0L

    val grouped = timeSeries.foldLeft(mutable.LinkedHashMap[(String, Long), List[V]]()) {
      case (zero, ((pk, _), value)) =>

        if (!(timeFunc(value) >= startRow && timeFunc(value) <= endRow)) {
          first = true
        }
        if (first) {
          startRow = timeFunc(value)
          endRow = timeFunc(value) + windowMillis
          first = false
        }

        zero.update((pk, endRow), zero.getOrElse((pk, endRow), List[V]()) :+ value)
        zero

    }.iterator

    grouped
  }

  private[task] def calculateStats(locationId: String, endTime: LocalDateTime, groupedData: List[TempSensorData]): SensorReport = {
    val sensorReport = SensorReport(timeSlotStart = endTime, location = locationId)
    val presence = groupedData.count(_.channelType == "presence")
    if (presence > 0) {
      val temperatures = groupedData.filter(_.channelType == "temperature")

      sensorReport.tempMin = Util.fahrenheitToCelsius(temperatures.minBy(_.value).value)
      sensorReport.tempMax = Util.fahrenheitToCelsius(temperatures.maxBy(_.value).value)
      sensorReport.tempAvg = Util.fahrenheitToCelsius(temperatures.aggregate(0.0)(
        (z: Double, d: (TempSensorData)) => z + d.value,
        (a: Double, b: Double) => a + b
      ) / temperatures.length)
      sensorReport.tempCnt = temperatures.length
      sensorReport.presence = true
      sensorReport.presenceCnt = presence
    }
    sensorReport
  }

  private[task] def recalculateStats(locationId: String, endTime: LocalDateTime, groupedData: List[SensorReport]): SensorReport = {
    val sensorReport = SensorReport(timeSlotStart = endTime, location = locationId)
    val presence = groupedData.count(_.presence == true)
    if (presence > 0) {
      val tempCount = groupedData.aggregate(0)((z, s) => z + s.tempCnt, (c, c1) => c + c1)

      sensorReport.tempMin = groupedData.minBy(_.tempMin).tempMin
      sensorReport.tempMax = groupedData.maxBy(_.tempMax).tempMax
      sensorReport.tempAvg = Util.trimDecimal(groupedData.aggregate(0.0)(
        (z: Double, d: (SensorReport)) => z + d.tempAvg,
        (a: Double, b: Double) => a + b
      ) / groupedData.length, 2)
      sensorReport.tempCnt = tempCount
      sensorReport.presence = true
      sensorReport.presenceCnt = presence
    }
    sensorReport
  }

}
