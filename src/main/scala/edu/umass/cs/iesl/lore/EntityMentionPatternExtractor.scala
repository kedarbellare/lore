package edu.umass.cs.iesl.lore

import org.riedelcastro.nurupo.Util
import java.io.{PrintWriter, File}
import io.{Codec, Source}
import scala.collection.JavaConversions._

/**
 * @author kedar
 */

trait MentionPatternExtractor extends ParallelProcessor {
  val annotator = new CoreNLPAnnotator

  import JobCenter._

  def inputJob = Job(new File(inputDirname), outputPrefix)

  override def debugEvery = 10

  // stanford corenlp pipeline is not thread-safe!!
  override def numWorkers = 1

  def generateWork(job: Job) = Util.files(job.inputFile).filter(_.getName.endsWith(".txt")).toIterator.map(Work(_))

  def inputDirname: String

  def outputPrefix: String
}

case class EntityMentionPatternExtractor(inputDirname: String, outputPrefix: String) extends MentionPatternExtractor {
  def name = "entityMentionExtractor"

  def doWork(input: Any, inputParams: Any, partialOutputWriter: PrintWriter, partialOutputParams: Any) {
    val inputFile = input.asInstanceOf[File]
    val source = Source.fromFile(inputFile, Codec.ISO8859.name())
    try {
      val filePath = inputFile.getAbsolutePath
      val docText = source.getLines().mkString("\n")
      for (line <- annotator.getEntityMentionPatterns(filePath, docText)) partialOutputWriter.println(line)
    } catch {
      case e: Exception => {
        logger.error("Error while processing file[" + inputFile.getAbsolutePath + "]: " + e.getMessage)
      }
    }
    source.close()
  }
}

object ExtractEntityMentionPatterns {
  def main(args: Array[String]) {
    EntityMentionPatternExtractor(args(0), args(1)).run
  }
}

case class RelationMentionPatternExtractor(inputDirname: String, outputPrefix: String) extends MentionPatternExtractor {
  def name = "relationMentionExtractor"

  def doWork(input: Any, inputParams: Any, partialOutputWriter: PrintWriter, partialOutputParams: Any) {
    val inputFile = input.asInstanceOf[File]
    val source = Source.fromFile(inputFile, Codec.ISO8859.name())
    try {
      val filePath = inputFile.getAbsolutePath
      val docText = source.getLines().mkString("\n")
      for (line <- annotator.getRelationMentionPatterns(filePath, docText)) partialOutputWriter.println(line)
    } catch {
      case e: Exception => {
        logger.error("Error while processing file[" + inputFile.getAbsolutePath + "]: " + e.getMessage)
      }
    }
    source.close()
  }
}

object ExtractRelationMentionPatterns {
  def main(args: Array[String]) {
    RelationMentionPatternExtractor(args(0), args(1)).run
  }
}