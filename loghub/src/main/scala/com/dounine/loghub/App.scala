package com.dounine.loghub

import java.io.{File, PrintWriter, RandomAccessFile}
import java.time._
import java.time.format.DateTimeFormatter
import java.util
import java.util.Collections
import java.util.concurrent.{ForkJoinPool, TimeUnit}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.native.Serialization.{read, write}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Random

object HbasePool {
  println("connecting.")
  private val connection: Connection = ConnectionFactory.createConnection(HBaseConfiguration.create())
  println("connected.")
  private val poolSize = 10
  private val pools = new util.HashMap[Int, BufferedMutator]()

  (0 until poolSize).foreach(
    x => {
      println("table connecting.")
      val params = new BufferedMutatorParams(TableName.valueOf("test_arc"))
      params.setWriteBufferPeriodicFlushTimeoutMs(TimeUnit.SECONDS.toMillis(3))
      params.setWriteBufferPeriodicFlushTimerTickMs(100)
      params.maxKeyValueSize(10485760)
      params.writeBufferSize(1024 * 1024 * 2)
      val table = connection.getBufferedMutator(params)

      pools.put(x, table)
      println("table connected.")
    }
  )

  def getTable: BufferedMutator = {
    pools.get(Random.nextInt(poolSize))
  }
}

object App {
  implicit val formats = org.json4s.DefaultFormats
  val seekFile = new File("/Users/huanghuanlai/dounine/github/billioncomputer/loghub/src/main/resources/seek.txt")
  val seekDayFile = new File("/Users/huanghuanlai/dounine/github/billioncomputer/loghub/src/main/resources/seekDay.txt")
  val logPath = new File("/Users/huanghuanlai/dounine/github/billioncomputer/loghub/log")

  Array(seekFile, seekDayFile).filter(!_.exists()).foreach(_.createNewFile())

  val offsets = new util.HashMap[String, Long]()
  offsets.putAll(seeks())


  def files(file: File, filter: File => Boolean): Seq[File] = {
    val listFiles = file.listFiles()
    listFiles.filter(filter) ++ listFiles.filter(_.isDirectory).flatMap(f => files(f, filter))
  }

  def seeks(): util.Map[String, Long] = {
    val seekStr = Source.fromFile(seekFile)
      .getLines.mkString
    if (seekStr.nonEmpty && seekStr != "") {
      read[Map[String, Long]](seekStr)
        .asJava
    } else new util.HashMap[String, Long]()
  }

  def readSeek(filePath: String): Long = {
    val fileMd5 = MD5Hash.getMD5AsHex(filePath.getBytes)
    val list = seeks().asScala.filter(_._1.equals(fileMd5))
    if (list.isEmpty) {
      writeSeek(Map(filePath -> 0L).asJava)
      0L
    } else list.head._2.toLong
  }

  def writeSeekDay(seekDay: String): Unit = {
    val writer = new PrintWriter(seekDayFile)
    writer.write(seekDay)
    writer.flush()
    writer.close()
  }

  def writeSeek(filePath: String, seek: Long): Unit = synchronized {
    writeSeek(Collections.singletonMap(filePath, seek))
  }

  def writeSeek(filePaths: util.Map[String, Long]): Unit = synchronized {
    val writer = new PrintWriter(seekFile)
    val convertList = filePaths.asScala.map(x => MD5Hash.getMD5AsHex(x._1.getBytes) -> x._2)
    val sets = offsets.asScala ++ convertList
    val seeksStr = write(sets)
    writer.write(seeksStr)
    writer.flush()
    writer.close()
    offsets.putAll(convertList.asJava)
  }

  def lines(file: File, startSeek: Long, endSeek: Long, finish: () => Unit): Iterator[String] = {
    new Iterator[String] {
      var rfile = new RandomAccessFile(file, "r")
      rfile.seek(startSeek)
      var nextLine: String = _
      var readSeek: Long = 0

      def appendSeek(): Unit = {
        if (nextLine != null) {
          nextLine = new String(nextLine.getBytes("ISO-8859-1"), "utf-8")
          readSeek += nextLine.getBytes.length
        }
      }

      override def hasNext: Boolean = {
        if (rfile == null) return false
        nextLine = rfile.readLine()
        appendSeek()
        val hl = nextLine != null && readSeek <= (endSeek - startSeek)
        if (!hl) {
          rfile.close()
          rfile = null
          finish()
        }
        hl
      }

      override def next(): String = {
        readSeek += 1 //append '\n' byte length
        nextLine
      }
    }
  }

  implicit class LDT(time: Long) {
    def getLocalDateTime: LocalDateTime = {
      Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime
    }
    def getLocalDate: LocalDate = {
      getLocalDateTime.toLocalDate
    }
    def getLocalTime: LocalTime = {
      getLocalDateTime.toLocalTime
    }
  }

  def run(logPath: File, defaultOffsetDay: String): Unit = {
    val sdfstr = Source.fromFile(seekDayFile).getLines().mkString
    val offsetDay = Option(if (sdfstr == "") null else sdfstr)

    val noneOffsetFold = logPath
      .listFiles()
      .filter(_.getName >= LocalDate.parse(offsetDay.getOrElse(defaultOffsetDay)).minusDays(1).toString)
      .sortBy(f => LocalDate.parse(f.getName).toEpochDay)

    val filesPar = noneOffsetFold
      .flatMap(files(_, file => file.getName.endsWith(".log")))
      .map(file => (file, seeks().getOrDefault(MD5Hash.getMD5AsHex(file.getAbsolutePath.getBytes()), 0), file.length()))
      .filter(tp2 => {
        val fileMd5 = MD5Hash.getMD5AsHex(tp2._1.getAbsolutePath.getBytes())
        val result = offsets.asScala.filter(m => fileMd5.equals(m._1))
        result.isEmpty || tp2._3 > result.head._2
      })
      .par

    filesPar.tasksupport = pool
    val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")
    val willUpdateOffset = new util.HashMap[String, Long]()
    var logTime:String = null
    filesPar
      .foreach(tp3 => {
        val hbaseClient = HbasePool.getTable
        lines(tp3._1, tp3._2, tp3._3, () => {
          willUpdateOffset.put(tp3._1.getAbsolutePath, tp3._3)
          offsets.put(MD5Hash.getMD5AsHex(tp3._1.getAbsolutePath.getBytes), tp3._3)
          //writeSeek(tp3._1.getAbsolutePath, tp3._3)
        })
          .foreach(line => {

            val jsonObject = parse(line)

            val time = (jsonObject \ "time").extract[Long]

            val data = jsonObject \ "data"

            val dataMap = data.values.asInstanceOf[Map[String, Any]]
              .filter(_._2 != null)
              .map(x => x._1 -> x._2.toString)

            val uid = dataMap("uid")
            logTime = time.getLocalDateTime.toString
            val rowkey = uid.take(2) + "|" + time.getLocalDateTime.format(formatter) + "|" + uid.substring(2, 8)

            val row = new Put(Bytes.toBytes(rowkey))
            dataMap.foreach(tp2 => row.addColumn(Bytes.toBytes("info"), Bytes.toBytes(tp2._1), Bytes.toBytes(tp2._2)))
            println(rowkey)

            hbaseClient.mutate(row)
          })
        hbaseClient.flush()
      })
    writeSeek(willUpdateOffset)
    writeSeekDay(noneOffsetFold.last.getName)
    //write logTime offset to mysql
  }

  val pool = new collection.parallel.ForkJoinTaskSupport(new ForkJoinPool(10))

  def main(args: Array[String]): Unit = {
    val logPath = new File("/Users/huanghuanlai/dounine/github/billioncomputer/loghub/log")

    val offsetDay = "2019-03-04"

    while(true){

      run(logPath, offsetDay)
      TimeUnit.SECONDS.sleep(1)
    }

  }

}


