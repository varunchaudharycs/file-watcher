import java.nio.file._
import java.time._
import java.io._
import java.security._

import scala.sys.process._
import scala.collection.JavaConverters._
import scala.collection._
import scala.io._
import scala.collection.mutable.ListBuffer
import java.net._

import util.control.Breaks._

// NOT WORKING !!!
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.commons.io.IOUtils


object test
{
  // GLOBAL VARIABLES
  val dir = "/home/infoobjects/Desktop/filewatcher/"
  val hdir = "/input/filewatcher/"
  var filelist = mutable.Map[String, ListBuffer[String]]()

  val start = "/usr/local/hadoop/sbin/start-all.sh"

  var file: String = "file.txt" // FILE NAME
  var id: String = "" // HASH VALUE
  var modified: Boolean = false

  var startup : Boolean = false

  // ---------------------- MAIN METHOD ----------------------
  def main(args: Array[String]): Unit =
  {
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), hadoopConf)

    val srcPath = new Path("/home/infoobjects/Desktop/file.txt")
    val destPath = new Path("/input/filewatcher")

    hdfs.copyFromLocalFile(srcPath, destPath)

  }

}