package com.dounine.compute

import com.dounine.compute.Structs.{DT, LogCase}
import com.dounine.compute.indicators.common.{PV, UV}
import com.dounine.compute.util.Convert._
import com.dounine.compute.util.{ConfUtil, HbaseUtil, SessionUtil, Tables}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.SparkSession

class App {

  def run(spark: SparkSession, start: String, end: String): Unit = {
    val sc = spark.sparkContext
    val conf = ConfUtil.getConf

    conf.set(TableInputFormat.INPUT_TABLE, Tables.LOG_TABLE)

    conf.set("hbase.security.authorization", "kerberos")
    conf.set("hbase.kerberos.principal", "admin/admin@dounine.com")
    conf.set("hbase.kerberos.keytab", "/etc/security/keytabs/admin.keytab")

    conf.set("hbase.table.split.startkey", "0")
    conf.set("hbase.table.split.startkey", "f")
    conf.setInt("hbase.table.split.radix", 16)
    conf.set("hbase.table.split.concat", "|")

    conf.set("TableInputFormat.SCAN_ROW_START", start)
    conf.set("TableInputFormat.SCAN_ROW_START", end)

    println(s"compute ======> $start to $end")
    spark.sparkContext.setCallSite(s"compute ======> $start to $end")

    import spark.implicits._
    val logDS = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat2],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
      .map(tp2 => HbaseUtil.resultToMap(tp2._2))
      .map(map => {
        LogCase(
          dt = DT(
            map.get("time").toLocalDateTimeStr(),
            map.get("time").toLocalDate().toString
          ),
          `type` = map.get("type"),
          aid = map.get("aid"),
          uid = map.get("uid"),
          tid = map.get("tid"),
          ip = map.get("ip")
        )
      }).toDS()

    logDS.cache()

    logDS.createTempView("log")

    new PV().run()
    new UV().run()

    logDS.unpersist()

    spark.catalog.dropTempView("log")
    spark.catalog.clearCache()


  }

}