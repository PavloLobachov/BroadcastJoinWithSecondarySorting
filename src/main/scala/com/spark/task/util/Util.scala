package com.spark.task.util

import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

import org.apache.spark.SparkContext

import scala.util.{Failure, Success, Try}

/**
  * Created by RLU on 12/6/2016.
  */
object Util {

  def fahrenheitToCelsius(temp: Double): Double = {
    trimDecimal((temp - 32) / 1.8, 2)
  }

  def trimDecimal(dec: Double, scale: Int): Double = {
    BigDecimal(dec).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def validatePath(path: String, master: String, sc: SparkContext): Unit = {
    if (master.startsWith("local")) {
      if (!Util.isPathExists(path)) {
        sc.stop()
        throw new FileNotFoundException(path)
      }
    } else {
      if (!Util.isHdfsPathExists(path, sc.hadoopConfiguration)) {
        sc.stop()
        throw new FileNotFoundException(path)
      }
    }
  }

  def isPathExists(path: String): Boolean = {
    require(path.nonEmpty, "Path can't be empty")
    Files.exists(Paths.get(path))
  }

  def isHdfsPathExists(path: String, conf: org.apache.hadoop.conf.Configuration): Boolean = {
    require(path.nonEmpty, "Path can't be empty")
    require(conf != null, "Configuration can't be null")
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(path))
    exists
  }

  def stringToLocalDateTime(date: String, format: String): LocalDateTime = {
    val formatter =
      new DateTimeFormatterBuilder().appendPattern(format)
        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
        .toFormatter()
    LocalDateTime.parse(date, formatter)
  }

  def stringToLocalDateTime(str: String): LocalDateTime = {
    Try(LocalDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME)) match {
      case Success(v) =>
        v
      case Failure(_) =>
        Try(new java.sql.Timestamp(java.sql.Date.valueOf(str).getTime)) match {
          case Success(v) =>
            v.toLocalDateTime
          case Failure(ex) =>
            throw new Exception(ex)
        }
    }
  }

  def millsToLocalDateTime(mills: Long): LocalDateTime = {
    new Timestamp(mills).toLocalDateTime
  }

  def localDateTimeToMills(date: LocalDateTime): Long = {
    Timestamp.valueOf(date).getTime
  }

}
