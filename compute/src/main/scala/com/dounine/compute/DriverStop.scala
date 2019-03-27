package com.dounine.compute

import com.dounine.compute.util._

object DriverStop {
  def main(args: Array[String]): Unit = {
    RedisUtil().getResource.publish("computeListenerMessage", "stop")
  }

}
