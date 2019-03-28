package com.dounine.compute.indicators.common

import com.dounine.compute.Structs.{DT, EXT, LogCase}
import com.dounine.compute.indicators.Indicator
import com.dounine.compute.util._
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.JavaConverters._

class Come extends Indicator {

  override def des(): String = "3/5/7留存"

  override def run(): Unit = {
    val spark = SessionUtil.getSparkSession
    val logDS = spark.table("log").as(ExpressionEncoder[LogCase])
    import spark.implicits._
    logDS
      .filter(_.uid != null)
      .mapPartitions(partitionT => {
        val cacheClient = DBHbaseHelper.getDBHbase(Tables.CACHE_TABLE)
        val userClient = DBHbaseHelper.getDBHbase(Tables.USER_TABLE)
        val md5ForCache = (log: LogCase) => MD5Hash.getMD5AsHex(s"${log.dt.date}|${log.uid}|come".getBytes)
        val md5ForUser = (log: LogCase) => MD5Hash.getMD5AsHex(s"${log.uid}".getBytes)
        partitionT
          .grouped(Consts.BATCH_MAPPARTITIONS)
          .flatMap { tList =>
            val cacheResult = tList
              .zip(cacheClient.incrments(tList.map(md5ForCache)))
              .map(tp2 => {
                val log = tp2._1
                log.copy(ext = EXT(render = tp2._2))
              })

            val userResult = userClient
              .getTable()
              .get(
                tList.map(md5ForUser)
                  .map(x =>
                    new Get(Bytes.toBytes(x)).addColumn(Bytes.toBytes("info"), Bytes.toBytes("regTime"))
                  )
                  .asJava
              )
              .map(HbaseUtil.resultToMap)
              .map(_.get("regTime"))

            cacheResult
              .zip(userResult)
              .map(tp2 => {
                val log = tp2._1
                log.copy(dt = DT(log.dt.time, log.dt.date, tp2._2))
              })
          }
      }).createTempView("comeTable")

    spark.sql(
      """
        |SELECT
        |    aid,
        |    dt.date,
        |    SUM(IF(dt.date = date_add(dt.regTime,3),1,0)) as come3,
        |    SUM(IF(dt.date = date_add(dt.regTime,5),1,0)) as come5,
        |    SUM(IF(dt.date = date_add(dt.regTime,7),1,0)) as come7
        |FROM
        |    comeTable
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
            Update(incs = Map(
              "come3" -> row.getAs[Long]("come3").toString,
              "come5" -> row.getAs[Long]("come5").toString,
              "come7" -> row.getAs[Long]("come7").toString
            )),
            "come_report"
          )
        })
        dbClient.close()
      })
  }
}
