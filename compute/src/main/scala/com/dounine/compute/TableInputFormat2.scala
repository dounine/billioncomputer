package com.dounine.compute

import java.io.IOException
import java.lang

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSplit}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{InputSplit, JobContext}
import org.apache.hadoop.security.UserGroupInformation
import scala.collection.JavaConverters._

class TableInputFormat2 extends TableInputFormat {

  override protected def initialize(context: JobContext): Unit = {
    System.setProperty("java.security.krb5.conf", getConf.get("java.security.krb5.conf","/etc/krb5.conf"))
    val tableName: TableName = TableName.valueOf(getConf.get(TableInputFormat.INPUT_TABLE))
    if ("kerberos".equals(getConf.get("hbase.security.authorization"))) {
      UserGroupInformation.setConfiguration(getConf)
      UserGroupInformation.loginUserFromKeytab(getConf.get("hbase.kerberos.principal"), getConf.get("hbase.kerberos.keytab"))
    }
    val connection = ConnectionFactory.createConnection(getConf)

    try
      this.initializeTable(connection, tableName)
    catch {
      case var4: Exception => throw var4
    }
  }

  @throws(classOf[IOException])
  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    this.initialize(context)
    val conf = context.getConfiguration
    val start = conf.get(TableInputFormat.SCAN_ROW_START, "")
    val end = conf.get(TableInputFormat.SCAN_ROW_STOP, "")
    val splitStart = conf.get("hbase.table.split.startkey", "0")
    val splitEnd = conf.get("hbase.table.split.endkey", "")
    val rowkeyRadix = conf.getInt("hbase.table.split.radix", 16)
    val rowkeyConcat = conf.get("hbase.table.split.concat", "")
    val regionSplits = conf.get("hbase.table.split.list", "")
    val numLength = Math.max(splitEnd.length, 1)
    val preString = "000000000000000000000000000000"

    if (StringUtils.isNotBlank(regionSplits) || StringUtils.isNoneBlank(splitEnd)) {
      var repls: Array[String] = null
      if (StringUtils.isNotBlank(regionSplits)) {
        repls = regionSplits.trim.split(",", -1)
      } else {
        if (rowkeyRadix == 10 || rowkeyRadix == 16) {
          repls = (lang.Long.valueOf(splitStart, rowkeyRadix).toInt to lang.Long.valueOf(splitEnd, rowkeyRadix).toInt)
            .map(x => if (rowkeyRadix == 16) Integer.toHexString(x) else x.toString)
            .map(i => s"$preString$i".takeRight(numLength))
            .toArray
        } else throw new Exception(rowkeyRadix + " => radix only working in ( 16 | 8 )")
      }
      repls
        .map {
          prefix =>
            val location = getRegionLocator.getRegionLocation(Bytes.toBytes(prefix))

            val splitStart: Array[Byte] = Bytes.add(Bytes.toBytes(prefix + rowkeyConcat), Bytes.toBytes(start))
            val splitStop: Array[Byte] = Bytes.add(Bytes.toBytes(prefix + rowkeyConcat), Bytes.toBytes(end))
            new TableSplit(getTable.getName, this.getScan, splitStart, splitStop, location.getHostname).asInstanceOf[InputSplit]
        }.toList.asJava
    } else {
      super.getSplits(context)
    }
  }

}
