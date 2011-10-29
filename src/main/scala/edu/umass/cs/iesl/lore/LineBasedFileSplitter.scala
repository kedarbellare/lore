package edu.umass.cs.iesl.lore

import io.Source
import java.io.{PrintWriter, File}

/**
 * @author kedar
 */


case class LineBasedFileSplitter(inputFilename: String, outputPrefix: String) extends ParallelProcessor {
  import JobCenter._

  def name = "fileSplit"

  def inputJob = Job(new File(inputFilename), outputPrefix)

  def generateWork(job: Job) = for (line <- Source.fromFile(job.inputFile).getLines()) yield Work(line)

  def doWork(input: Any, inputParams: Any, partialOutputWriter: PrintWriter, partialOutputParams: Any) {
    partialOutputWriter.println(input)
  }
}

object SplitFileByLines {
  def main(args: Array[String]) {
    LineBasedFileSplitter(args(0), args(1)).run
  }
}