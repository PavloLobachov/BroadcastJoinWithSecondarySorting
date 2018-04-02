package com.spark.task.configuration

/**
  * Created by RLU on 5/15/2017.
  */
case class ParserConfiguration(mode:String = "", sensorDevicesDataFile: String = "", sensorDataFile: String = "",
                               sensorReportFile: String = "",targetDirectory: String = "",
                               numOfRooms: Int = 1, from: Long = 0, until: Long = 0)