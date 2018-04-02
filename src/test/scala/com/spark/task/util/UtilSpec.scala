package com.spark.task.util

import java.io.File

import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen}

/**
  * Created by pavlolobachov on 3/24/18.
  */
class UtilSpec extends FlatSpec with GivenWhenThen with BeforeAndAfterAll {

  behavior of "UtilSpec"

  private val file = File.createTempFile("pre-", ".txt")
  private val path: String = file.getAbsolutePath

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    file.deleteOnExit()
  }

  it should "convert temp fahrenheit to celsius and round double to 2 decimal places" in {
    Given("Temp in Fahrenheit")
    val inputTemperature = -459.67
    val expectedTemperature = -273.15
    When("Method executed")
    val temp = Util.fahrenheitToCelsius(inputTemperature)
    Then("Temperature converted to celsius")
    assertResult(temp)(expectedTemperature)
  }

  it should "check whether file exist" in {
    Given("File path")
    When("Method executed")
    val isExist = Util.isPathExists(path)
    Then("Flag returned")
    assertResult(isExist)(true)
  }

  it should "raise exception for empty argument" in {
    Given("Empty file path")
    val path = ""
    When("Method executed")
    intercept[IllegalArgumentException] {
      Then("Exception raised")
      Util.isPathExists(path)
    }
  }

  it should "convert date string to LocalDateTime instance with specified format" in {
    Given("Date string")
    val testDate = "2017-03-31T12:53:37"
    When("Method executed")
    val testLocalDateTime = Util.stringToLocalDateTime(testDate, "yyyy-MM-dd'T'HH:mm:ss")
    Then("LocalDateTime object returned")
    assertResult(testLocalDateTime.getYear)(2017)
    assertResult(testLocalDateTime.getMonthValue)(3)
    assertResult(testLocalDateTime.getDayOfMonth)(31)
    assertResult(testLocalDateTime.getHour)(12)
    assertResult(testLocalDateTime.getMinute)(53)
    assertResult(testLocalDateTime.getSecond)(37)
  }

  it should "convert date string to LocalDateTime instance" in {
    Given("Date string")
    val testDate = "2017-03-31T12:53:37"
    When("Method executed")
    val testLocalDateTime = Util.stringToLocalDateTime(testDate)
    Then("LocalDateTime object returned")
    assertResult(testLocalDateTime.getYear)(2017)
    assertResult(testLocalDateTime.getMonthValue)(3)
    assertResult(testLocalDateTime.getDayOfMonth)(31)
    assertResult(testLocalDateTime.getHour)(12)
    assertResult(testLocalDateTime.getMinute)(53)
    assertResult(testLocalDateTime.getSecond)(37)
  }

  it should "convert date mills to LocalDateTime instance" in {
    Given("Date mills")
    val testDateMills = 1490954017000L
    When("Method executed")
    val testLocalDateTime = Util.millsToLocalDateTime(testDateMills)
    Then("LocalDateTime object returned")

    assertResult(testLocalDateTime.getYear)(2017)
    assertResult(testLocalDateTime.getMonthValue)(3)
    assertResult(testLocalDateTime.getDayOfMonth)(31)
    assertResult(testLocalDateTime.getHour)(12)
    assertResult(testLocalDateTime.getMinute)(53)
    assertResult(testLocalDateTime.getSecond)(37)
  }

  it should "convert LocalDateTime instance to mills" in {
    Given("LocalDateTime instance")
    val testDateMills = Util.millsToLocalDateTime(1490954017000L)
    When("Method executed")
    val testLocalDateTimeMills = Util.localDateTimeToMills(testDateMills)
    Then("mills returned")
    assertResult(testLocalDateTimeMills)(1490954017000L)
  }
}
