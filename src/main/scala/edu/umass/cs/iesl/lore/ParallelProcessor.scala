package edu.umass.cs.iesl.lore

import akka.actor.Actor._
import akka.util.duration._
import akka.actor.{Channel, Actor}
import akka.dispatch.Dispatchers

import System.{currentTimeMillis => now}
import org.riedelcastro.nurupo.HasLogger
import java.io.{PrintWriter, File}

/**
 * @author kedar
 */

object JobCenter {

  case class Job(inputFile: File, outputPrefix: String, inputParams: Any = null)

  case object JobStart

  case class Work(input: Any, inputParams: Any = null)

  case object WorkDone

  case object JobDone

  case class JobResult(outputParams: Any = null)

}

trait ParallelProcessor extends HasLogger {

  import JobCenter._

  def name: String

  def parallelName: String = "parallel[" + name + "]"

  def debugEvery: Int = 10000

  def preRun() {
    logger.info("")
    logger.info("Starting " + parallelName)
    // shutdown all actors before starting: bad!!
    // Actor.registry.shutdownAll()
  }

  def inputJob: Job

  // given a job generate the work
  def generateWork(job: Job): Iterator[Work]

  // new output parameters (for master/worker)
  def newOutputParams(isMaster: Boolean = false): Any = null

  // actual work: should be atomic and not change global data structures
  def doWork(input: Any, inputParams: Any, partialOutputWriter: PrintWriter, partialOutputParams: Any)

  // merge partial output parameters
  def merge(outputParams: Any, partialOutputParams: Any) {}

  // number of workers (can be overridden say for thread-unsafe processing)
  def numWorkers: Int = math.min(Conf.get[Int]("max-workers", 1), Runtime.getRuntime.availableProcessors())

  class Worker(val outputPrefix: String) extends Actor with HasLogger {
    self.dispatcher = Dispatchers.newThreadBasedDispatcher(self)
    
    val partialOutputWriter = {
      val outputFilename = outputPrefix + System.getProperty("file.separator") + self.uuid.toString
      val outputFile = new File(outputFilename)
      // make directory if necessary
      logger.info("Worker writing to path " + outputFilename)
      outputFile.getParentFile.mkdirs()
      new PrintWriter(outputFile)
    }
    // initialize partial output
    val partialOutputParams = newOutputParams()

    protected def receive = {
      case work: Work => {
        // process the work
        doWork(work.input, work.inputParams, partialOutputWriter, partialOutputParams)
        // reply to master
        self reply WorkDone
      }
      case JobDone => {
        self reply JobResult(partialOutputParams)
        self.stop()
      }
    }

    override def postStop() {
      partialOutputWriter.close()
    }
  }

  class Master extends Actor with HasLogger {
    val nrOfWorkers = numWorkers
    val workIterator = generateWork(inputJob)
    var nrOfMessages = 0
    var nrOfDones = 0
    var nrOfResults = 0
    val outputParams = newOutputParams(true)

    // create the workers
    val workers = Vector.fill(nrOfWorkers)(actorOf(new Worker(inputJob.outputPrefix)).start())

    var origRecipient: Option[Channel[Any]] = None

    protected def receive = {
      case JobStart => {
        logger.info("Master starting job")
        origRecipient = Some(self.channel)
        for (worker <- workers) {
          if (workIterator.hasNext) {
            worker ! workIterator.next()
            nrOfMessages += 1
          } else {
            worker ! JobDone
          }
        }
      }
      case WorkDone => {
        nrOfDones += 1
        if (nrOfDones % debugEvery == 0) {
          logger.info("Master received #dones=" + nrOfDones + "/#messages=" + nrOfMessages)
        }
        if (workIterator.hasNext) {
          self reply workIterator.next()
          nrOfMessages += 1
        } else {
          self reply JobDone
        }
      }
      case result: JobResult => {
        // merge output params with partial
        merge(outputParams, result.outputParams)
        logger.info("Master received #dones=" + nrOfDones + "/#messages=" + nrOfMessages)        
        // increment #results
        nrOfResults += 1
        logger.info("Master received #result=" + nrOfResults + "/#workers=" + nrOfWorkers)
        if (nrOfResults == nrOfWorkers) {
          require(origRecipient.isDefined)
          origRecipient.get ! JobResult(outputParams)
          self.stop()
        }
      }
    }
  }

  def run = {
    preRun()

    // create master
    val master = actorOf(new Master).start()

    // start the job
    val start = now

    // send job to master
    val outputParams = master.?(JobStart)(timeout = Actor.Timeout(30 days)).await.resultOrException match {
      // wait for result with a long timeout!! (30 days!!!)
      case Some(result) => {
        logger.info("Completed " + parallelName + " in time=" + (now - start) + " millis")
        result.asInstanceOf[JobResult].outputParams
      }
      case None => {
        logger.error("Failed to complete " + parallelName + " after time=" + (now - start) + " millis")
        throw new RuntimeException("Job " + inputJob + " -> " + parallelName + " failed!!!")
      }
    }

    // return merged output parameters
    outputParams
  }
}