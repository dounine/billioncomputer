package com.dounine.compute.util

import org.apache.spark.sql.SparkSession

object SessionUtil {

  private var sparkSession: SparkSession = _

  def init(sparkSession: SparkSession): Unit = {
    this.sparkSession = sparkSession
  }

  def getSparkSession: SparkSession = {
    sparkSession
  }

}
