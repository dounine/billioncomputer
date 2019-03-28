package com.dounine.compute

object Structs {

  case class DT(var time: String, var date: String, var regTime: String = null)

  case class EXT(var render: Long = 0)

  case class LogCase(
                      dt: DT,
                      `type`: String,
                      aid: String,
                      uid: String,
                      tid: String,
                      ip: String,
                      ext: EXT = EXT()
                    )

}
