package com.dounine.compute.util

import java.util

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

class DBHbase(tableName: String) {

  private val connection = ConnectionFactory.createConnection(ConfUtil.getConf)
  private val table = connection.getTable(TableName.valueOf(tableName))

  def getTable(): Table ={
    table
  }

  def incrments(incs: Seq[String], family: String = "info", amount: Int = 1): Seq[Long] = {
    if (incs.isEmpty) {
      Seq[Long]()
    } else {
      require(incs.head.length == 32, "pk require 32 length")
      val convertIncs = incs map { pk => new Increment(Bytes.toBytes(pk.take(8))).addColumn(Bytes.toBytes(family), Bytes.toBytes(pk.takeRight(24)), amount) }
      val results = new Array[Object](convertIncs.length)
      table.batch(convertIncs.asJava, results)
      results.array.indices.map(
        ind =>
          Bytes.toLong(
            results(ind)
              .asInstanceOf[Result]
              .getValue(
                Bytes.toBytes(family),
                Bytes.toBytes(incs(ind).takeRight(24))
              )
          )
      )
    }
  }
}

object DBHbaseHelper {

  private val maps = new util.HashMap[String, DBHbase]()

  def getDBHbase(tableName: String): DBHbase = this.synchronized {
    var db = maps.get(tableName)
    if (null == db) {
      db = new DBHbase(tableName)
      maps.put(tableName, db)
    }
    db
  }

}
