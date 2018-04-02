package com.spark.task

import java.io.{File, IOException}
import java.nio.file.Paths

import com.spark.task.containers.{SensorReport, TempSensorData}
import com.spark.task.util.Util
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen}

import scala.io.Source

/**
  * Created by pavlolobachov on 3/29/18.
  */
class ApplicationReportReprocessingSpec extends FlatSpec with GivenWhenThen with BeforeAndAfterAll {

  behavior of "TestApplicationReportReprocessingSpec"

  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  val conf = new SparkConf().setAppName("test1").setMaster("local[4]")
  val targetDirectory = "/tmp/_spark_test_0"

  override protected def beforeAll(): Unit = {
    _sc = new SparkContext(conf)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    sc.stop()
    System.clearProperty("spark.driver.port")
    _sc = null
    super.afterAll()
  }

  it should "Generate 1 hour and 15 min reports" in {
    Given("SensorData and SensorDevice data files")
    val sensorDeviceFile = new File(this.getClass.getClassLoader.getResource("ds1.csv").toURI).getPath
    val sensorDataFile = new File(this.getClass.getClassLoader.getResource("ds2.csv").toURI).getPath
    val numOfPartitions = 1 // should be equal to number of rooms

    When("Report generation run")
    ApplicationReportReprocessing.processReports(sc, numOfPartitions, sensorDeviceFile, sensorDataFile, targetDirectory)

    Then("Two reports are generated")
    val expectedFifteenMinReport = Array("2018-03-23T15:11:16", "Room 3", "14.63", "21.3", "17.97", "8", "true", "1")
    val expectedOneHourReport = Array("2018-03-23T16:11:16", "Room 3", "14.63", "21.3", "17.97", "8", "true", "1")

    val fifteenMinReport = fileToList(targetDirectory + "/15m_Report/part-00000")
    val oneHourReport = fileToList(targetDirectory + "/1H_Report/part-00000")

    assertResult(1)(fifteenMinReport.length)
    assertResult(1)(oneHourReport.length)

    assert(fifteenMinReport.head === expectedFifteenMinReport)
    assert(oneHourReport.head === expectedOneHourReport)
    purgeLocalFiles(targetDirectory)
  }

  it should "Generate 1 hour and 15 min reports with 72 records each based on specified date range" in {
    Given("SensorData and SensorDevice data files")
    val sensorDeviceFile = new File(this.getClass.getClassLoader.getResource("ds1.csv").toURI).getPath
    val sensorDataFile = new File(this.getClass.getClassLoader.getResource("ds2.csv").toURI).getPath
    val numOfPartitions = 1 // should be equal to number of rooms
    val from = Util.localDateTimeToMills(Util.stringToLocalDateTime("2018-03-23"))
    val until = Util.localDateTimeToMills(Util.stringToLocalDateTime("2018-03-25"))

    When("Report generation run")
    ApplicationReportReprocessing.processReports(sc, numOfPartitions, sensorDeviceFile, sensorDataFile, targetDirectory, from, until)

    Then("Two reports are generated")
    val expectedFifteenMinReport = Array("2018-03-23T15:11:16", "Room 3", "14.63", "21.3", "17.97", "8", "true", "1")
    val expectedOneHourReport = Array("2018-03-23T16:11:16", "Room 3", "14.63", "21.3", "17.97", "8", "true", "1")

    val fifteenMinReport = fileToList(targetDirectory + "/15m_Report/part-00000")
    val oneHourReport = fileToList(targetDirectory + "/1H_Report/part-00000")

    assertResult(288)(fifteenMinReport.length)
    assertResult(72)(oneHourReport.length)

    assert(fifteenMinReport.head === expectedFifteenMinReport)
    assert(oneHourReport.head === expectedOneHourReport)
    purgeLocalFiles(targetDirectory)
  }

  it should "Generate 1 hour report based on 15 min report" in {
    Given("Sensor report file")
    val sensorReportFile = new File(this.getClass.getClassLoader.getResource("15m_Report.csv").toURI).getPath
    val numOfPartitions = 1 // should be equal to number of rooms
    val expectedOneHourReport = Array("2018-03-23T13:13:11", "Room 3", "14.4", "20.79", "17.92", "399", "true", "4")

    When("Report generation run")
    ApplicationReportReprocessing.reprocessReport(sc, numOfPartitions, sensorReportFile, targetDirectory)

    Then("One hour report is generated")
    val oneHourReport = fileToList(targetDirectory + "/1H_Report/part-00000")
    assertResult(1)(oneHourReport.length)
    assert(oneHourReport.head === expectedOneHourReport)
    purgeLocalFiles(targetDirectory)
  }

  it should "Generate 1 hour report with 24 records based on 15 min report with specified date range" in {
    Given("Sensor report file")
    val sensorReportFile = new File(this.getClass.getClassLoader.getResource("15m_Report.csv").toURI).getPath
    val numOfPartitions = 1 // should be equal to number of rooms
    val expectedOneHourReport = Array("2018-03-23T13:13:11", "Room 3", "14.4", "20.79", "17.92", "399", "true", "4")
    val from = Util.localDateTimeToMills(Util.stringToLocalDateTime("2018-03-23"))
    val until = Util.localDateTimeToMills(Util.stringToLocalDateTime("2018-03-23"))

    When("Report generation run")
    ApplicationReportReprocessing.reprocessReport(sc, numOfPartitions, sensorReportFile, targetDirectory, from, until)

    Then("One hour report is generated")
    val oneHourReport = fileToList(targetDirectory + "/1H_Report/part-00000")
    assertResult(24)(oneHourReport.length)
    assert(oneHourReport.head === expectedOneHourReport)
    purgeLocalFiles(targetDirectory)
  }

  it should "Add empty records to RDD based on the window size, from and until dates" in {
    Given("SensorReport RDD")
    val groupedData = List(
      SensorReport(Util.stringToLocalDateTime("2018-03-23T12:13:11"), "Room 3", 18.15, 18.67, 18.38, 21, true, 3),
      SensorReport(Util.stringToLocalDateTime("2018-03-23T12:30:41"), "Room 3", 15.86, 19.32, 17.88, 117, true, 6),
      SensorReport(Util.stringToLocalDateTime("2018-03-23T12:46:01"), "Room 3", 15.08, 19.25, 17.74, 135, true, 9),
      SensorReport(Util.stringToLocalDateTime("2018-03-23T13:01:06"), "Room 3", 14.4, 20.79, 17.67, 126, true, 6)
    )
    val from = Util.localDateTimeToMills(Util.stringToLocalDateTime("2018-03-23"))
    val until = Util.localDateTimeToMills(Util.stringToLocalDateTime("2018-03-23"))

    val sensorReportRDD = sc.parallelize(groupedData, 1)

    When("Function executed")
    val expectedResult = 96
    val realResult = ApplicationReportReprocessing.addEmptyRecords(sensorReportRDD, ApplicationReportReprocessing.FifteenMinutesWindow, from, until + ApplicationReportReprocessing.DayToMilliseconds).count()

    Then("SensorReport object returned and contains all stats")
    assert(realResult == expectedResult)
  }

  it should "Calculate stats based on time window grouped data" in {
    Given("Time window grouped data")
    val groupedData = List(
      TempSensorData("Sensor 7", 100, "presence", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:57:21"), 1),
      TempSensorData("Sensor 7", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:57:21"), 70.34746726485808),
      TempSensorData("Sensor 8", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:57:21"), 58.73782280173701)
    )
    val expectedResult = SensorReport(
      Util.millsToLocalDateTime(1521809901000L),
      "Room 3",
      Util.fahrenheitToCelsius(58.73782280173701),
      Util.fahrenheitToCelsius(70.34746726485808),
      Util.fahrenheitToCelsius((58.73782280173701 + 70.34746726485808) / 2),
      2, true, 1
    )
    When("Function executed")
    val realResult = ApplicationReportReprocessing.calculateStats("Room 3", Util.millsToLocalDateTime(1521809901000L), groupedData)

    Then("SensorReport object returned and contains all stats")
    assert(realResult === expectedResult)
  }

  it should "Calculate empty stats when there is no presence" in {
    Given("Time window grouped data")
    val groupedData = List(
      TempSensorData("Sensor 7", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:57:21"), 70.34746726485808),
      TempSensorData("Sensor 8", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:57:21"), 58.73782280173701)
    )
    val expectedResult = SensorReport(Util.millsToLocalDateTime(1521809901000L), "Room 3")
    When("Function executed")
    val realResult = ApplicationReportReprocessing.calculateStats("Room 3", Util.millsToLocalDateTime(1521809901000L), groupedData)

    Then("SensorReport object returned and contains all stats")
    assert(realResult === expectedResult)
  }

  it should "Recalculate stats based on time window grouped data" in {
    Given("Time window grouped data")
    val groupedData = List(
      SensorReport(Util.stringToLocalDateTime("2018-03-23T12:13:11"), "Room 3", 18.15, 18.67, 18.38, 21, true, 3),
      SensorReport(Util.stringToLocalDateTime("2018-03-23T12:30:41"), "Room 3", 15.86, 19.32, 17.88, 117, true, 6),
      SensorReport(Util.stringToLocalDateTime("2018-03-23T12:46:01"), "Room 3", 15.08, 19.25, 17.74, 135, true, 9),
      SensorReport(Util.stringToLocalDateTime("2018-03-23T13:01:06"), "Room 3", 14.4, 20.79, 17.67, 126, true, 6)
    )

    val expectedResult = SensorReport(Util.stringToLocalDateTime("2018-03-23T13:13:11"), "Room 3", 14.4, 20.79, 17.92, 399, true, 4)
    When("Function executed")
    val realResult = ApplicationReportReprocessing.recalculateStats("Room 3", Util.stringToLocalDateTime("2018-03-23T13:13:11"), groupedData)

    Then("SensorReport object returned and contains all stats")
    assert(realResult === expectedResult)
  }

  it should "Group data by specified time window in minutes" in {
    Given("Input data partitioned by locationId and sorted by timestamp")
    val roomPartitionedData = List(
      (("Room 3", 1521809776000L), TempSensorData("Sensor 8", 300, "presence", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:16"), 1))
      , (("Room 3", 1521809776000L), TempSensorData("Sensor 7", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:16"), 70.31344345911896))
      , (("Room 3", 1521809776000L), TempSensorData("Sensor 8", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:16"), 58.3652246813368))
      , (("Room 3", 1521809781000L), TempSensorData("Sensor 7", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:21"), 70.09880150319741))
      , (("Room 3", 1521809781000L), TempSensorData("Sensor 8", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:21"), 58.41216983166317))
      , (("Room 3", 1521809796000L), TempSensorData("Sensor 7", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:36"), 70.17377091083415))
      , (("Room 3", 1521809796000L), TempSensorData("Sensor 8", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:36"), 58.334556177156855))
      , (("Room 3", 1521809841000L), TempSensorData("Sensor 7", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:57:21"), 70.34746726485808))
      , (("Room 3", 1521809841000L), TempSensorData("Sensor 8", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:57:21"), 58.73782280173701))
    ).iterator

    val timeWindowGroupedData = List(
      (("Room 3", 1521809836000L),
        List(TempSensorData("Sensor 8", 300, "presence", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:16"), 1)
          , TempSensorData("Sensor 7", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:16"), 70.31344345911896)
          , TempSensorData("Sensor 8", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:16"), 58.3652246813368)
          , TempSensorData("Sensor 7", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:21"), 70.09880150319741)
          , TempSensorData("Sensor 8", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:21"), 58.41216983166317)
          , TempSensorData("Sensor 7", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:36"), 70.17377091083415)
          , TempSensorData("Sensor 8", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:56:36"), 58.334556177156855))),
      (("Room 3", 1521809901000L),
        List(TempSensorData("Sensor 7", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:57:21"), 70.34746726485808)
          , TempSensorData("Sensor 8", 100, "temperature", "Room 3", Util.stringToLocalDateTime("2018-03-23T14:57:21"), 58.73782280173701)))
    )
    When("Window specified")
    val actualResult = ApplicationReportReprocessing.groupByTimeRange[TempSensorData](roomPartitionedData, 1, (tsd: TempSensorData) => Util.localDateTimeToMills(tsd.timestamp))
    Then("Data should be grouped by specified window")
    assert(timeWindowGroupedData === actualResult.toList)
  }

  @throws[IOException]
  def purgeLocalFiles(path: String) {
    if (path != null) {
      val node: File = Paths.get(path).normalize.toFile
      // Only purge state when it's under /tmp.  This is a safety net to prevent accidentally
      // deleting important local directory trees.
      if ((SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC_OSX) && node.getAbsolutePath.startsWith("/tmp")) {
        FileUtils.deleteDirectory(node)
      } else if ((SystemUtils.IS_OS_WINDOWS_7 || SystemUtils.IS_OS_WINDOWS_8) && node.getAbsolutePath.startsWith("C:\\tmp")) {
        FileUtils.deleteDirectory(node)
      }
    }
  }

  private def readFile(path: String) = {
    Source.fromFile(path).getLines()
  }

  private def fileToList(path: String): Seq[Array[String]] = {
    readFile(path).map(line => line.trim.split(",")).toSeq
  }

}
