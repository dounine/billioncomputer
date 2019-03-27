package com.dounine.compute

import java.time.Duration
import java.time.format.DateTimeFormatter
import java.util.concurrent.{Executors, TimeUnit}

import com.dounine.compute.util._
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.JedisPubSub

object DriverMain {
  System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
  val spark = SparkSession
    .builder()
    .appName("compute")
    .master("local")
    .getOrCreate()
  SessionUtil.init(spark)
  var appRunning: Boolean = true
  val executor = Executors.newFixedThreadPool(2)

  listenerMessage()

  def listenerMessage(): Unit = {
    val channelName = "computeListenerMessage"
    executor.submit(new Runnable {
      override def run(): Unit = {
        val red = RedisUtil().getResource
        val sub = new JedisPubSub {
          override def onMessage(channel: String, message: String): Unit = {
            if ("stop".equals(message)) {
              println(s"receive $message")
              appRunning = false
              this.unsubscribe()
            } else {
              println(s"message = $message")
            }
          }
        }
        val t = new Thread(() => {
          red.subscribe(sub, channelName)
        })
        t.setDaemon(true)
        t.start()
        while (!Thread.currentThread().isInterrupted) {
          TimeUnit.SECONDS.sleep(1)
        }
        t.stop()
      }
    })
  }


  def main(args: Array[String]): Unit = {
    start()
    executor.shutdownNow()
    spark.stop()
    System.exit(0)
  }

  def start(): Unit = {
    while (appRunning) {
      val props = PropsUtils.properties("db")
      val dbClient = new DBJdbc(props.getProperty("jdbcUrl"))
      val loghubResultSet = dbClient.query(
        """
          |SELECT
          |    time
          |FROM
          |    offset
          |WHERE
          |    offsetName = 'loghub'
        """.stripMargin)

      val indicatorResultSet = dbClient.query(
        """
          |SELECT
          |    time
          |FROM
          |    offset
          |WHERE
          |    offsetName = 'indicator'
        """.stripMargin)

      val loghubTime = if (loghubResultSet.next()) {
        loghubResultSet.getTimestamp("time").toLocalDateTime.minusMinutes(3)
      } else sys.error("loghub offset not found")

      val indicatorTime = if (indicatorResultSet.next()) {
        indicatorResultSet.getTimestamp("time").toLocalDateTime
      } else sys.error("indicator offset not found")

      val betweenTimeMinutes = Duration.between(indicatorTime, loghubTime).toMinutes

      val app = new App()
      val format = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")

      if (betweenTimeMinutes >= 1) {
        app.run(spark, indicatorTime.format(format), loghubTime.format(format))

        dbClient.upsert(Map("offsetName" -> "indicator"), Update(sets = Map("time" -> loghubTime.toString)), "offset")
      } else {
        TimeUnit.SECONDS.sleep(3)
      }
      loghubResultSet.close()
      indicatorResultSet.close()
      dbClient.close()
    }

  }

}
