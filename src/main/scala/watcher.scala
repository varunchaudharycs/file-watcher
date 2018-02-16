import java.nio.file._
import java.io._
import java.net.URI
import java.security._
import java.util.Calendar

import scala.sys.process._
import scala.collection.JavaConverters._
import scala.collection._
import scala.io._
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import util.control.Breaks._

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
  var nonreadable : Boolean = false

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

  if (!listing.isEmpty)
  {
    for (i <- listing)
    {
      if (!i.endsWith(".tlr") && i.charAt(0) != '.' && i(i.length() - 1) != '~')
      {
        create(i)
        backupHDFS(i)
      }
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
        id = hashValue(file)
        // CHECK IF MODIFY (HASH VALUE OF UNMODIFIED FILE)
        var oldhash = getHash(file)

        if(oldhash.length == 0)
          create(file)
        else
        {
            // LOG
            logwriter.write(Calendar.getInstance().getTime + " -> " + "FILE: " + file + " - MODIFIED\n")
            logwriter.flush()

            modified = true

            delete(file, oldhash)
            create(file)

            modified = false
        }

      }
      // DELETE
      else if(kind.equals(StandardWatchEventKinds.ENTRY_DELETE))
      {
        println("Entry DELETED: " + file)

        id = getHash(file)

        delete(file, id)
      }

    } // FILE HANDLED

  } // PRINT EVENT METHOD ENDS

  // ---------------------- DELETE ----------------------
  def delete(file : String, id : String)
  {
    // DELETE FILE NAME FROM LIST
    filelist(id) --= ListBuffer(file)

    // CHECK IF NO FILES EXIST WITH SAME HASH
    if(filelist(id).isEmpty)
      filelist.remove(id)

    // DELETE TRAILER FILE
    new File(dir + file + ".tlr").delete()

    // CHANGE DIRECTORY IN HDFS -> backup (FILE && TRAILER)
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), hadoopConf)

    hdfs.rename(new Path(hdir + file), new Path(dir + "backup/"))
    hdfs.rename(new Path(hdir + file + ".tlr"), new Path(dir + "backup/"))


    // UPDATE LOG (IF NOT MODIFIED)
    if(!modified)
    {
      logwriter.write(Calendar.getInstance().getTime + " -> " + "FILE: " + file + " - DELETED\n")
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

      if(new File(dir + file).length() == 0)
      {
        println("Empty File - No trailer file created")

        // HDFS BACKUP
        backupHDFS(file)
      }
      else
      {
        // CHECK FOR DUPLICATE
        if(filelist.contains(id))
        {

          println("FILE WAS A DUPLICATE")

          // CHECK FOR TRAILER
          if(!Files.exists(Paths.get(dir + file + ".tlr")))
          {
            var f : File = new File(dir + file + ".tlr")

            // COPY TRAILER FILE (DUPLICATE)
            Files.copy(Paths.get(dir + filelist(id).head + ".tlr"), Paths.get(dir + file + ".tlr"))
          }

          // ADD FILE TO LIST
          filelist(id) ++= ListBuffer(file)

          backupHDFS(file)

        }
        // ORIGINAL FILE
        else
        {
          // CHECK FOR TRAILER
          if (!Files.exists(Paths.get(dir + file + ".tlr")))
          {
            println("sup?")

            // CREATING TRAILER FILE (ORIGINAL)
            writer = new PrintWriter(new FileOutputStream(new File(dir + file + ".tlr"), true))

            // TRAILER FILE OPERATIONS
            writer.write("Hash value = " + id)
            writer.flush()

            numberOfLines(file)
            highestFrequencyWord(file)
            backupHDFS(file)

            writer.close()
          }
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
      nonreadable = true
      backupHDFS(file)
      nonreadable = false
    }

    // CHECKING FOR STARTUP UPDATE + MODIFIED
    if(!startup && !modified)
    {
      logwriter.write(Calendar.getInstance().getTime + " -> " + "FILE: " + file + " - CREATED\n")
      logwriter.flush()
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

    id = sha.digest.map("%02x".format(_)).mkString

    return id

  } // HASH VALUE METHOD ENDS

  // ---------------------- NUMBER OF LINES ----------------------
  def numberOfLines(file : String)
  {
    writer.write("\nNumber of lines = " + Source.fromFile(dir + file).getLines.size);
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

    hdfs.copyFromLocalFile(new Path(dir + file), new Path(hdir))

    if(!nonreadable)
      hdfs.copyFromLocalFile(new Path(dir + file + ".tlr"), new Path(hdir))


  } // HDFS BACKUP METHOD ENDS

  // ---------------------- OLD HASH ----------------------
  def getHash(file : String) : String =
  {
    for (entry <- filelist.entrySet)
    {
      val key = entry.getKey
      val value = entry.getValue

      for (filelistvalues <- value)
        if (filelistvalues.equals(file))
          return key
    } // OUTER FOR

    return ""
  }
}