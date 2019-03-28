package com.dounine.compute.indicators.user

import com.dounine.compute.indicators.Indicator
import com.dounine.compute.util._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConverters._

class User extends Indicator {
  override def des(): String = "用户表"

  override def run(): Unit = {
    val spark = SessionUtil.getSparkSession

    spark.sql(
      """
        |SELECT
        |    aid,
        |    uid,
        |    tid,
        |    dt.time as regTime
        |FROM
        |    log
        |WHERE
        |     type = "register"
      """.stripMargin)
      .rdd
      .foreachPartition(rows => {
        val hbaseClient = DBHbaseHelper.getDBHbase(Tables.USER_TABLE)
        rows
          .grouped(1000)
          .foreach(crows => {
            val batchPut = crows
              .map(HbaseUtil.rowToMap)
              .map(map => {
                val put = new Put(Bytes.toBytes(map.get("uid")))
                map.asScala.foreach(kv => {
                  put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(kv._1), Bytes.toBytes(kv._2))
                })
                put
              })
            hbaseClient.getTable().put(batchPut.asJava)
          })
      })
  }
}
