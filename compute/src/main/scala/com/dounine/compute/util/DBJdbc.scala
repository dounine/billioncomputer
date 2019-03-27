package com.dounine.compute.util

import java.sql._
import java.time.{LocalDate, LocalDateTime}

import scala.collection.mutable.ListBuffer


class DBJdbc(jdbcUrl: String) {
  private var connection: Connection = _
  val driver = "com.mysql.jdbc.Driver"

  init()

  def init(): Unit = {
    if (connection == null || connection.isClosed) {
      Class.forName(driver)
      connection = DriverManager.getConnection(jdbcUrl)
    }
  }

  def close(): Unit = {
    if (!connection.isClosed) {
      connection.close()
    }
  }

  def execute(sql: String, params: Any*): Unit = {
    try {
      val statement = connection.prepareStatement(sql)
      this.fillStatement(statement, params: _*)
      statement.executeUpdate
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
  }

  @throws[SQLException]
  def fillStatement(statement: PreparedStatement, params: Any*): Unit = {
    params.indices
      .foreach(j => {
        val value: Any = params(j)
        val i = j+1
        value match {
          case _:String => statement.setString(i, value.toString)
          case _:Integer => statement.setInt(i, value.toString.asInstanceOf[Int])
          case _:Boolean => statement.setBoolean(i, value.toString.asInstanceOf[Boolean])
          case _:LocalDate => statement.setString(i, value.toString)
          case _:LocalDateTime => statement.setString(i, value.toString)
          case _:Long => statement.setLong(i, value.toString.asInstanceOf[Long])
          case _:Double => statement.setDouble(i, value.toString.asInstanceOf[Double])
          case _:Float => statement.setFloat(i, value.toString.asInstanceOf[Float])
          case _ => statement.setString(i, String.valueOf(value))
        }
      })
  }

  def upsert(query: Map[String,String], update: Update, tableName: String): Unit = {
    val names = ListBuffer[String]()
    val values = ListBuffer[String]()
    val params = ListBuffer[AnyRef]()
    val updates = ListBuffer[AnyRef]()
    val keysArr = scala.Array(query.keys, update.sets.keys, update.incs.keys)
    val valuesArr = scala.Array(update.sets.values, update.incs.values)
    keysArr.indices.foreach(i => {
      val item = keysArr(i)
      item.foreach {
        key => {
          names += s"`$key`"
          values += "?"
        }
      }
      i match {
        case 0 =>
          params.++=(query.values)
        case 1 | 2 =>
          params.++=(valuesArr(i - 1).toList)
      }
    })
    update.sets.foreach {
      item => {
        updates += s" `${item._1}` = ? "
        params += item._2
      }
    }
    update.incs.foreach {
      item => {
        updates += s" `${item._1}` = `${item._1}` + ? "
        params += item._2
      }
    }
    val sql = s"INSERT INTO `$tableName` (${names.mkString(",")}) VALUES(${values.mkString(",")}) ON DUPLICATE KEY UPDATE ${updates.mkString(",")}"
    this.execute(sql, params.toArray[AnyRef]: _*)
  }

  def query(sql: String): ResultSet = {
    connection.createStatement().executeQuery(sql)
  }

  def prepareStatement(sql: String): PreparedStatement = {
    connection.prepareStatement(sql)
  }
}

case class Update(sets: Map[String, String] = Map(), incs: Map[String, String] = Map())


