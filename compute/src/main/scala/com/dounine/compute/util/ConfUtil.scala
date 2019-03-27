package com.dounine.compute.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

object ConfUtil {

  def getConf: Configuration = {
    val conf = HBaseConfiguration.create()

    conf
  }

}
