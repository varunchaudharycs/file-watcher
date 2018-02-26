import java.nio.file._
import java.time._
import java.io._
import java.security._

import scala.sys.process._
import scala.collection.JavaConverters._
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.io._
import scala.collection.mutable.ListBuffer
import java.net._

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark._
import org.apache.spark.SparkContext._
import watcher.{dir, hdir}
//import util.control.Breaks._

// NOT WORKING !!!
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.commons.io.IOUtils

import java.util.Calendar
import scala.collection.JavaConversions._
import java.util
import scala.collection.JavaConversions._

object test
{
  // GLOBAL VARIABLES
  val dir = "/home/infoobjects/Desktop/filewatcher/"
  val hdir = "/input/filewatcher/"
  var filelist = mutable.Map[String, ArrayBuffer[String]]()
  filelist = mutable.Map("1" -> ArrayBuffer("10"), "2" -> ArrayBuffer("20"))
  val start = "/usr/local/hadoop/sbin/start-all.sh"

  var file: String = "file.txt" // FILE NAME
  var id: String = "" // HASH VALUE
  var modified: Boolean = false

  var startup : Boolean = false

  // ---------------------- MAIN METHOD ----------------------
  def main(args: Array[String]): Unit =
  {

  }
}