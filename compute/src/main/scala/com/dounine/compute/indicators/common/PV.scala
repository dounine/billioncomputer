package com.dounine.compute.indicators.common

import com.dounine.compute.indicators.Indicator
import com.dounine.compute.util.{DBJdbc, PropsUtils, SessionUtil, Update}

class PV extends Indicator {

  override def des(): String = "PV"

  override def run(): Unit = {
    val spark = SessionUtil.getSparkSession

    spark.sql(
      """
        |SELECT
        |    aid,
        |    dt.date,
        |    COUNT(1) as pv
        |FROM
        |    log
        |GROUP BY
        |    aid,
        |    dt.date
      """.stripMargin)
      .rdd
      .foreachPartition(rows => {
        val props = PropsUtils.properties("db")
        val dbClient = new DBJdbc(props.getProperty("jdbcUrl"))
        rows.foreach(row => {
          dbClient.upsert(
            Map(
              "time" -> row.getAs[String]("date"),
              "aid" -> row.getAs[String]("aid")
            ),
            Update(incs = Map("pv" -> row.getAs[Long]("pv").toString)),
            "common_report"
          )
        })
        dbClient.close()
      })
  }

}
