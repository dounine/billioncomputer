package com.dounine.compute.util

import java.io.IOException
import java.util
import java.util.Properties

object PropsUtils {

  private val PROPERTIES_MAP = new util.HashMap[String, Properties]

  def properties(name: String): Properties = {
    var properties = PROPERTIES_MAP.get(name)
    if (null == properties) try {
      properties = new Properties
      properties.load(getClass().getResourceAsStream("/" + name + ".properties"))
      PROPERTIES_MAP.put(name, properties)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    properties
  }
}
