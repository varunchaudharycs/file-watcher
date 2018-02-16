import java.nio.file._
import java.time._
import java.io._
import java.net.URI
import java.security._

import scala.sys.process._
import scala.collection.JavaConverters._
import scala.collection._
import scala.io._
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import util.control.Breaks._

// NOT WORKING !!!
/*import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path*/

object watcher
{
  // GLOBAL VARIABLES
  val dir = "/home/infoobjects/Desktop/filewatcher/"
  val hdir = "/input/filewatcher/"
  var filelist = mutable.Map[String, ListBuffer[String]]()

  val start = "/usr/local/hadoop/sbin/start-all.sh"

  var file : String = ""    // FILE NAME
  var id : String = ""      // HASH VALUE
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

  if(!listing.isEmpty)
  {
    for(i <- listing)
    {
      if(!i.endsWith(".tlr") && i.charAt(0) != '.' && i(i.length()-1) != '~')
        create(i)
    }
  }

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
      path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE)

      while (true)
      {
        // RECEIVING WatchKey INSTANCE
        val watchKey = watchService.take()

        // CALLING METHOD TO IMPLEMENT EVENT ORIENTED TASKS
        watchKey.pollEvents().asScala.foreach( e => { startup = false; printEvent(e) })

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
      writer.close()

      // STOPPING HADOOP SERVICES
      //  "/usr/local/hadoop/sbin/stop-all.sh".!

    }

  } // MAIN METHOD ENDS

  // ---------------------- EVENT TASKS ----------------------
  def printEvent(event : WatchEvent[_]) : Unit =
  {
    val kind = event.kind
    file = event.context().asInstanceOf[Path].toString   // FILE NAME

    // CHECKING FOR TXT FILE & IGNORING TRAILER + HIDDEN FILES + BACKUP FILES
    if(!file.endsWith(".tlr") && file.charAt(0) != '.' && file(file.length()-1) != '~')
    {

      // FIND HASH VALUE
      if(!kind.equals(StandardWatchEventKinds.ENTRY_DELETE))
        id = hashValue(file)

      // ADD      
      if(kind.equals(StandardWatchEventKinds.ENTRY_CREATE))
      {
        modified = false

        writer = new PrintWriter(new FileOutputStream(new File(dir + file + ".tlr"), true))

        // CHECK IF CREATE OR MODIFY
        if(Files.exists(Paths.get(dir + file)))
        {
          logwriter.write("FILE : " + file + " was CREATED at " + LocalDateTime.now() +  "\n")
          logwriter.flush()

          modified = true

          delete(file)
          create(file)
        }
        else
        {
          create(file)
        }

        writer.close()
      }
      // DELETE
      else if(kind.equals(StandardWatchEventKinds.ENTRY_DELETE))
      {
        println("Entry DELETED: " + file)

        delete(file)
      }

    } // FILE HANDLED

  } // PRINTEVENT METHOD ENDS

  // ---------------------- DELETE ----------------------
  def delete(file : String)
  {
    // MODIFIED ?
    if(modified)
    {
      // DELETE OLD HASH FROM FILE LIST
    }
    else
    // NOT MODIFIED ? DELETE HASH FROM LIST
    {
      filelist(id) --= ListBuffer(file)
    }

    // CHECK IF NO FILES EXIST WITH SAME HASH
    if(filelist(id).isEmpty)
      filelist.remove(id)

    // DELETE TRAILER FILE
    new File(dir + file + ".tlr").delete()

    // CHANGE DIRECTORY IN HDFS -> backup


    // UPDATE LOG (IF NOT MODIFIED)
    if(!modified)
    {
      logwriter.write("FILE : " + file + " was DELETED at " + LocalDateTime.now() +  "\n")
      logwriter.flush()
    }

  } // DELETE ENDS

  // ---------------------- CREATE ----------------------
  def create(file : String)
  {

    if(Files.probeContentType(new File(dir + file).toPath()).equals("text/plain"))
    {
      if(id.equals(""))
      {
        hashValue(file)
      }

      // CHECK FOR DUPLICATE
      if(filelist.contains(id))
      {
        println("FILE WAS A DUPLICATE")

        var f : File = new File(dir + file + ".tlr")

        // CHECK FOR TRAILER
        if(!f.exists())
        {
          // COPY TRAILER FILE (DUPLICATE)
          Files.copy(Paths.get(dir + filelist(id).head + ".tlr"),Paths.get(dir + file + ".tlr"))
        }

        // ADD FILE TO LIST
        filelist(id) ++= ListBuffer(file)

        backupHDFS(file)

      }
      // ORIGINAL FILE
      else
      {
        var f : File = new File(dir + file + ".tlr")
        // CHECK FOR TRAILER
        if(!f.exists())
        {
          // CREATING TRAILER FILE (ORIGINAL) && HDFS BACKUP
          writer.write("Hash value = " + id)
          writer.flush()

          numberOfLines(file)
          highestFrequencyWord(file)
          backupHDFS(file)
        }

        // ADDING TO LIST
        filelist += id -> ListBuffer(file)

      }
    }
    // NON-READABLE FILE
    else
    {
      println("FILE IS NON-READABLE")

      // HDFS BACKUP
      var cmd = "/usr/local/hadoop/bin/hadoop fs -put " + dir + file + " " + hdir
      cmd.!

    }

    // CHECKING FOR STARTUP UPDATE + MODIFIED
    if(!startup && !modified)
    {
      logwriter.write("FILE : " + file + " was CREATED at " + LocalDateTime.now() +  "\n")
      logwriter.flush()
    }

  } // CREATE ENDS

  // ---------------------- HASH VALUE ----------------------
  def hashValue(file : String) : String =
  {
    val buffer = new Array[Byte](8192)
    val sha = MessageDigest.getInstance("SHA")
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

    id = sha.digest.map("%02x".format(_)).mkString

    return id

  } // HASH VALUE METHOD ENDS

  // ---------------------- NUMBER OF LINES ----------------------
  def numberOfLines(file : String)
  {

   var lines = Source.fromFile(dir + file).getLines.size

    writer.write("\nNumber of lines = " + lines);
    writer.flush()

  } // NUMBER OF LINES METHOD ENDS

  // ---------------------- HIGHEST FREQUENCY WORD ----------------------
  def highestFrequencyWord(file : String)
  {

    writer.write("\nHighest frequency word = " + Source.fromFile(new File(dir + file)).getLines.flatMap(_.split("\\W+")).foldLeft(Map.empty[String, Int])
    {
      (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
    }.toSeq.sortWith(_._2 > _._2).head._1)

    writer.flush()

  } // HIGHEST FREQUENCY METHOD ENDS

  // ---------------------- HDFS BACKUP ----------------------
  def backupHDFS(file : String)
  {
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), hadoopConf)

    val srcPath = new Path(dir + file)
    val destPath = new Path(hdir)

    hdfs.copyFromLocalFile(srcPath, destPath)

  } // HDFS BACKUP METHOD ENDS

}