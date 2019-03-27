package com.dounine.compute.indicators.common

import com.dounine.compute.Structs.{EXT, LogCase}
import com.dounine.compute.indicators.Indicator
import com.dounine.compute.util._
import org.apache.hadoop.hbase.util.MD5Hash
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

class UV extends Indicator {

  override def des(): String = "UV"

  override def run(): Unit = {
    val spark = SessionUtil.getSparkSession
    val logDS = spark.table("log").as(ExpressionEncoder[LogCase])
    import spark.implicits._
    logDS
      .mapPartitions(partitionT => {
        val hbaseClient = DBHbaseHelper.getDBHbase(Tables.CACHE_TABLE)
        val md5 = (log: LogCase) => MD5Hash.getMD5AsHex(s"${log.dt.date}|${log.aid}|${log.uid}|uv2".getBytes)
        partitionT
          .grouped(Consts.BATCH_MAPPARTITIONS)
          .flatMap { tList =>
            tList
              .zip(hbaseClient.incrments(tList.map(md5)))
              .map(tp2 => {
                val log = tp2._1
                log.copy(ext = EXT(tp2._2))
              })
          }
      }).createTempView("uvTable")

    spark.sql(
      """
        |SELECT
        |    aid,
        |    dt.date,
        |    COUNT(1) as uv
        |FROM
        |    uvTable
        |WHERE
        |    ext.render = 1
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
            Update(incs = Map("uv" -> row.getAs[Long]("uv").toString)),
            "common_report"
          )
        })
        dbClient.close()
      })

    spark.catalog.dropTempView("uvTable")
  }
}
