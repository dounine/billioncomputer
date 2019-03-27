package com.dounine.compute.util

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

object HbaseUtil {

  def rowToMap(row: Row): java.util.Map[String, String] = {
    val maps = row.getValuesMap[Any](row.schema.fieldNames)
    maps.filter(_._2 != null).map(x => x._1 -> String.valueOf(x._2)).asJava
  }

  def resultToMap(result: Result): java.util.Map[String, String] = {
    result
      .rawCells()
      .filter(_.getValueLength != 0)
      .map(cell => Bytes.toString(CellUtil.cloneQualifier(cell)) -> Bytes.toString(CellUtil.cloneValue(cell)))
      .toMap
      .asJava
  }

}
