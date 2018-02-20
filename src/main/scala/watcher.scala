import java.io._
import java.net.URI
import java.security._
import java.nio.file._
import java.util.Calendar

import scala.io._
import scala.collection._
import scala.sys.process._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration

import util.control.Breaks._

object watcher
{
  // GLOBAL VARIABLES
  val dir = "/home/infoobjects/Desktop/filewatcher/"
  val hdir = "/input/filewatcher/"
  var trailerinfo = mutable.Map[String, ArrayBuffer[String]]()

  val start = "/usr/local/hadoop/sbin/start-all.sh"

  var file : String = ""    // FILE NAME
  var hash : String = ""      // HASH VALUE
  var oldhash : String = ""   // FOR MODIFY EVENT FOLLOWING CREATE(unnecessary)
  var modified : Boolean = false

  println("STARTING HADOOP SERVICES ...\n")
  start.!

  // LOG FILE
  var logwriter : PrintWriter = new PrintWriter(new FileOutputStream(new File("/home/infoobjects/Desktop/logs.txt"),true))

  // TRAILER FILE
  var writer : PrintWriter = null

  // UPDATING FILE LIST
  var listing : Array[String] = new File(dir).list

  // CHECK IF DIRECTORY ALREADY CONTAINS FILES
  var startup : Boolean = true

  // UPDATING FILE LIST && COVERING MISSING OPERATIONS
  if (!listing.isEmpty)
    for (i <- listing)
      if (!i.endsWith(".tlr") && i.charAt(0) != '.' && i(i.length() - 1) != '~')
        create(i)

  startup = false

  // ---------------------- MAIN METHOD ----------------------
  def main(args: Array[String])
  {
    try
    {
      println("\nWATCHING " + dir + "... \n")

      // CREATING WatchService
      val path = FileSystems.getDefault().getPath(dir)
      val watchService = path.getFileSystem().newWatchService()

      // REGISTERING PATH WITH KINDS OF EVENTS
      path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY)

      while (true)
      {
        // RECEIVING WatchKey INSTANCE
        val watchKey = watchService.take()

        // CALLING METHOD TO IMPLEMENT EVENT ORIENTED TASKS
        watchKey.pollEvents().asScala.foreach( e =>
        {
          printEvent(e)
        })

        // CHECK FOR CLOSED WATCH SERVICE [or closing of thread(if!)]
        if (!watchKey.reset())
        {
          println("WATCH SERVICE CLOSED / THREAD CLOSED")
          watchKey.cancel()
          watchService.close()
          break
        }

      } // while ENDS

    } // try ENDS

    catch
      {
        case e: Exception => e.printStackTrace()
      }

    finally
    {
      println("STOPPED WATCHING " + dir + " ...")

      // CLOSING PrintWriter OBJECTS
      logwriter.close()

      // STOPPING HADOOP SERVICES
      //  "/usr/local/hadoop/sbin/stop-all.sh".!

    }

  } // MAIN METHOD ENDS

  // ---------------------- EVENT TASKS ----------------------
  def printEvent(event : WatchEvent[_]) : Unit =
  {
    val kind = event.kind
    file = event.context().asInstanceOf[java.nio.file.Path].toString   // FILE NAME

    // CHECKING FOR TXT FILE & IGNORING TRAILER + HIDDEN FILES + BACKUP FILES
    if(!file.endsWith(".tlr") && file.charAt(0) != '.' && file(file.length()-1) != '~')
    {
      // CREATE || MODIFY
      if(kind.equals(StandardWatchEventKinds.ENTRY_CREATE))
      {
        println("Entry CREATED: " + file)

        hash = hashValue(file)

        create(file)

        // CHECK UNNECESSARY MODIFY EVENT FOR CREATED FILE
        oldhash = hash
      }
      // DELETE
      else if(kind.equals(StandardWatchEventKinds.ENTRY_DELETE))
      {
        println("Entry DELETED: " + file)

        delete(file)
      }
      else if(kind.equals(StandardWatchEventKinds.ENTRY_MODIFY))
      {
        // CHECK UNNECESSARY MODIFY EVENT FOR CREATED FILE
        if(!hashValue(file).equals(oldhash))
        {
          println("Entry MODIFIED: " + file)

          logwriter.write(Calendar.getInstance().getTime + " -> " + "FILE: " + file + " - MODIFIED\n")
          logwriter.flush()

          modified = true

          delete(file)

          create(file)

          modified = false
        }
      }

    } // FILE HANDLED

  } // PRINT EVENT METHOD ENDS

  // ---------------------- DELETE ----------------------
  def delete(file : String)
  {
    if(!modified)
    {
      // LOG
      logwriter.write(Calendar.getInstance().getTime + " -> " + "FILE: " + file + " - DELETED\n")
      logwriter.flush()
    }

    // CHANGE DIRECTORY IN HDFS -> backup/ (FILE && TRAILER)
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), hadoopConf)

    // CHECK IF FILE & TRAILER EXISTS IN BACKUP (to ADD || REPLACE)
    val fileHDFSpath = new org.apache.hadoop.fs.Path("/input/filewatcher/backup/" + file)
    val trailerHDFSpath = new org.apache.hadoop.fs.Path("/input/filewatcher/backup/" + file + ".tlr")

    // DELETE FILE to REPLACE
    if(hdfs.exists(fileHDFSpath))
      hdfs.delete(fileHDFSpath, true)

    // DELETE TRAILER (Local + HDFS) to REPLACE
    if(hdfs.exists(trailerHDFSpath))
      hdfs.delete(trailerHDFSpath, true)

    if(Files.exists(Paths.get(dir + file + ".tlr")))
    {
      new File(dir + file + ".tlr").delete()
      hdfs.rename(new org.apache.hadoop.fs.Path(hdir + file + ".tlr"), new org.apache.hadoop.fs.Path(hdir + "backup/" + file + ".tlr"))
    }

    println(hdir + file)

    // MOVE TO backup/
    hdfs.rename(new org.apache.hadoop.fs.Path(hdir + file), new org.apache.hadoop.fs.Path(hdir + "backup/" + file))

  } // DELETE ENDS

  // ---------------------- CREATE ----------------------
  def create(file : String)
  {
    // CHECKING FOR STARTUP UPDATE FOR LOGGING
    if(!startup && !modified)
    {
      logwriter.write(Calendar.getInstance().getTime + " -> " + "FILE: " + file + " - CREATED \n")
      logwriter.flush()
    }

    if(Files.probeContentType(new File(dir + file).toPath()).equals("text/plain"))
    {
      // CHECK - EMPTY FILE
      if(new File(dir + file).length() == 0)
      {
        // LOG
        logwriter.write("EMPTY FILE - NO TRAILER CREATED \n")
        logwriter.flush()

        // HDFS BACKUP
        backupHDFS(file, false)
      }
      else
      {
        // CHECK FOR DUPLICATE
        if(trailerinfo.contains(hash))
        {
          // LOG
          logwriter.write("DUPLICATE FILE \n")
          logwriter.flush()

          // CHECK FOR TRAILER
          if(!Files.exists(Paths.get(dir + file + ".tlr")))
          {
            writer = new PrintWriter(new FileOutputStream(new File(dir + file + ".tlr"), true))

            // WRITE CONTENTS OF TRAILER FILE FROM TRAILER INFO
            writer.write("Hash: " + hash + "\n")
            writer.write("Number of lines: " + trailerinfo(hash)(0) + "\n")
            writer.write("Highest frequency word: " + trailerinfo(hash)(1) + "\n")
            writer.flush()
          }

          // HDFS BACKUP
          backupHDFS(file, true)

        }
        // ORIGINAL FILE
        else
          // CHECK FOR TRAILER
          if (!Files.exists(Paths.get(dir + file + ".tlr")))
          {
            // CREATING TRAILER FILE (ORIGINAL)
            writer = new PrintWriter(new FileOutputStream(new File(dir + file + ".tlr"), true))

            // TRAILER FILE OPERATIONS
            writer.write("Hash = " + hash + "\n")
            writer.flush()

            numberOfLines(file)
            highestFrequencyWord(file)
            backupHDFS(file, true)

            writer.close()
          }
      }
    }
    // NON-READABLE FILE
    else
    {
      println("FILE IS NOT READABLE")

      backupHDFS(file, false)
    }

  } // CREATE ENDS

  // ---------------------- HASH VALUE ----------------------
  def hashValue(file : String) : String =
  {
    val buffer = new Array[Byte](8192)
    val sha = MessageDigest.getInstance("SHA-1")
    val dis = new DigestInputStream(new FileInputStream(new File(dir + file)), sha)

    // TO READ FILE IN CHUNKS SO ENTIRE CONTENT IS NOT HELD IN MEMORY(=> FASTER)
    try
    {
      while(dis.read(buffer) != -1) {}
    }

    finally
    {
      dis.close()
    }

    hash = sha.digest.map("%02x".format(_)).mkString

    return hash

  } // HASH VALUE METHOD ENDS

  // ---------------------- NUMBER OF LINES ----------------------
  def numberOfLines(file : String)
  {
    var lines = Source.fromFile(dir + file).getLines.size

    writer.write("Number of lines = " + lines + "\n");
    writer.flush()

    // ADD TO MAP
    trailerinfo += hash -> ArrayBuffer(lines.toString)

  } // NUMBER OF LINES METHOD ENDS

  // ---------------------- HIGHEST FREQUENCY WORD ----------------------
  def highestFrequencyWord(file : String)
  {
    var word = Source.fromFile(new File(dir + file)).getLines.flatMap(_.split("\\W+")).foldLeft(Map.empty[String, Int])
    {
      (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
    }.toSeq.sortWith(_._2 > _._2).head._1

    writer.write("Highest frequency word = " + word + "\n")

    writer.flush()

    // ADD TO MAP
    trailerinfo(hash) += word

  } // HIGHEST FREQUENCY METHOD ENDS

  // ---------------------- HDFS BACKUP ----------------------
  def backupHDFS(file : String, trailercreated : Boolean)
  {
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), hadoopConf)

    hdfs.copyFromLocalFile(new Path(dir + file), new Path(hdir))

    if(trailercreated)
      hdfs.copyFromLocalFile(new Path(dir + file + ".tlr"), new Path(hdir))

  } // HDFS BACKUP METHOD ENDS

}