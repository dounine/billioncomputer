package com.dounine.compute.util

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

object Convert {

  implicit class StringToLDT(time: String) {

    def toLocalDateTime(): LocalDateTime = {
      Instant.ofEpochMilli(time.toLong)
        .atZone(ZoneId.systemDefault())
        .toLocalDateTime
    }

    def toLocalDate(): LocalDate = {
      Instant.ofEpochMilli(time.toLong)
        .atZone(ZoneId.systemDefault())
        .toLocalDateTime
        .toLocalDate
    }

    def toLocalDateTimeStr(pattern: String = "yyyy-MM-dd'T'HH:mm:ss.SSS"): String = {
      toLocalDateTime()
        .format(DateTimeFormatter.ofPattern(pattern))
    }

  }

}
