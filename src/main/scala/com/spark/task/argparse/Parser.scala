package com.spark.task.argparse

import com.spark.task.configuration.ParserConfiguration
import com.spark.task.util.Util
import scopt.OptionParser

/**
  * Created by PavloL on 12/17/2016.
  */
object Parser {

  val dateRegex = raw"(\d{4})-(\d{2})-(\d{2})".r

  def getParser: OptionParser[ParserConfiguration] = {
    new scopt.OptionParser[ParserConfiguration]("TestSparkTask") {
      head("scopt", "3.x")

      checkConfig { c =>
        if (c.mode.nonEmpty) success else failure("A command [process|reprocess] [options] is required.")
      }

      cmd("process").required().action((_, c) => c.copy(mode = "process"))
        .text("Process 1 hour and 15 min reports").children(
        opt[String]('D', "sensor-devices-data-file").required().valueName("<sensor-devices-data-file>").
          action((x, c) => c.copy(sensorDevicesDataFile = x)).
          text(
            """Absolute path to sensor device data file.
            """.stripMargin).minOccurs(1).maxOccurs(1),
        opt[String]('d', "sensor-data-file").required().valueName("<sensor-data-file>").
          action((x, c) => c.copy(sensorDataFile = x)).
          text(
            """Absolute path to sensor data file.
            """.stripMargin).minOccurs(1).maxOccurs(1),

        opt[String]('t', "target-directory").required().valueName("<target-directory>").
          action((x, c) => c.copy(targetDirectory = x)).
          text(
            """Absolute path to directory where to save reports.
            """.stripMargin).minOccurs(1).maxOccurs(1),

          opt[Int]('n', "num-of-rooms").required().valueName("<num-of-rooms>").
          action((x, c) => c.copy(numOfRooms = x)).
          validate(
            {
              case cf if cf > 0 => success
              case other => failure(s"Number of rooms should be above 0")
            }).
          text(
            """Number of rooms to track.
            """.stripMargin).minOccurs(1).maxOccurs(1),

          opt[String]('f', "from-date").optional().valueName("<from-date>").
          action((x, c) => c.copy(from = Util.localDateTimeToMills(Util.stringToLocalDateTime(x)))).
          validate(
            {
              case dateRegex(_*) => success
              case _ => failure("Not a valid date patter")
            }).
          text(
            """Define lower date boundary for report generation.
              |Date pattern: yyyy-MM-dd
            """.stripMargin).minOccurs(0).maxOccurs(1),

          opt[String]('u', "until-date").optional().valueName("<until-date>").
          action((x, c) => c.copy(until = Util.localDateTimeToMills(Util.stringToLocalDateTime(x)))).
          validate(
            {
              case dateRegex(_*) => success
              case _ => failure("Not a valid date patter")
            }).
          text(
            """Define upper date boundary for report generation.
              |Date pattern: yyyy-MM-dd
            """.stripMargin).minOccurs(0).maxOccurs(1)
      ).minOccurs(0).maxOccurs(1)

      cmd("reprocess").required().action((_, c) => c.copy(mode = "reprocess"))
        .text("Reprocess 15 min report and produce 1 hour report file").children(
        opt[String]('r', "sensor-report-file").required().valueName("<sensor-report-file>").
          action((x, c) => c.copy(sensorReportFile = x)).
          text(
            """Absolute path 15 minutes report file.
            """.stripMargin).minOccurs(1).maxOccurs(1),

        opt[String]('t', "target-directory").required().valueName("<target-directory>").
          action((x, c) => c.copy(targetDirectory = x)).
          text(
            """Absolute path to directory where to save reports.
            """.stripMargin).minOccurs(1).maxOccurs(1),

        opt[Int]('n', "num-of-rooms").required().valueName("<num-of-rooms>").
          action((x, c) => c.copy(numOfRooms = x)).
          validate(
            {
              case cf if cf > 0 => success
              case other => failure(s"Number of rooms should be above 0")
            }).
          text(
            """Number of rooms to track.
            """.stripMargin).minOccurs(1).maxOccurs(1),

        opt[String]('f', "from-date").optional().valueName("<from-date>").
          action((x, c) => c.copy(from = Util.localDateTimeToMills(Util.stringToLocalDateTime(x)))).
          validate(
            {
              case dateRegex(_*) => success
              case _ => failure("Not a valid date patter")
            }).
          text(
            """Define lower date boundary for report generation.
              |Date pattern: yyyy-MM-dd
            """.stripMargin).minOccurs(0).maxOccurs(1),

        opt[String]('u', "until-date").optional().valueName("<until-date>").
          action((x, c) => c.copy(until = Util.localDateTimeToMills(Util.stringToLocalDateTime(x)))).
          validate(
            {
              case dateRegex(_*) => success
              case _ => failure("Not a valid date patter")
            }).
          text(
            """Define upper date boundary for report generation.
              |Date pattern: yyyy-MM-dd
            """.stripMargin).minOccurs(0).maxOccurs(1)
      ).minOccurs(0).maxOccurs(1)

      help("help").text("prints this usage text")

      version("version").text("0.0.1")
    }
  }
}
