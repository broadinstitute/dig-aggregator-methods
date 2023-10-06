package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._
import scala.util.matching.Regex

class MergeRegionsStage(implicit context: Context) extends Stage {
  import Implicits.S3Key

  val partitionFiles: Input.Source = Input.Source.Success(s"out/ldsc/regions/partitioned/*/")
  val biosamplePartitions: Input.Source = Input.Source.Success(s"out/ldsc/regions/merged/annotation-tissue-biosample/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(partitionFiles, biosamplePartitions)

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 200,
    applications = Seq.empty,
    stepConcurrency = 5
  )

  val datasetPlusPart: Regex = "([^/]+)/partition=([^/]+)/".r
  // Exchange memory for reducing the number of AWS calls which can be slow individually
  lazy val datasetMatches: Seq[Regex.Match] = context.s3
    .ls(s"${partitionFiles.prefix.commonPrefix}")
    .flatMap { obj =>
      datasetPlusPart.findFirstMatchIn(obj.key)
    }
  lazy val datasetToBiosampleMergeOutputMap: Map[String, Seq[String]] = datasetMatches
    .groupBy(_.group(1))
    .map { case (dataset, partitions) =>
      dataset -> partitions.map(m => BiosampleMergeOutput(m.group(2)).toOutput).distinct
    }

  // This is a pattern we can reuse: it is a cascade of runs of the same job which drills down
  // We ignore the dataset level itself because those can be used directly
  // Run once and you get merged biosample level data
  // Run a second time and you get merged tissue level data
  // Could continue, but we currently only use tissue level data (bioindex) and biosample data (annotation expression)
  override val rules: PartialFunction[Input, Outputs] = {
    case partitionFiles(dataset) =>
      // if there are no partitions, then we can ignore it
      datasetToBiosampleMergeOutputMap.getOrElse(dataset, Seq()) match {
        case Nil => Outputs.Null
        case partitionOutputs => Outputs.Named(partitionOutputs: _*)
      }
    case biosamplePartitions(partition) => Outputs.Named(TissueMergeOutput(partition).toOutput)
  }

  override def make(output: String): Job = {
    val mergeOutput = MergeOutput.fromOutput(output)
    new Job(Job.Script(
      resourceUri("mergeRegions.pl"),
      mergeOutput.partition,
      mergeOutput.partitionIn,
      mergeOutput.partitionOut
    ))
  }

  /** Update the success flag of the merged regions.
    */
  override def success(output: String): Unit = {
    context.s3.touch(s"${MergeOutput.fromOutput(output).outputDirectory}/_SUCCESS")
    ()
  }
}

case class MergeOutput(
  partition: String,
  partitionIn: String,
  partitionOut: String
) {
  def outputDirectory: String = s"out/ldsc/regions/$partitionOut"
}

case class BiosampleMergeOutput(partition: String) {
  val Seq(annotation, tissue, biosample, _) = partition.split("___").toSeq
  val inputPartition: String = s"${annotation}___${tissue}___${biosample}___*/part-*"
  val outputPartition: String = s"${annotation}___${tissue}___${biosample}"

  val partitionIn: String = s"partitioned/*/partition=$inputPartition"
  val partitionOut: String = s"merged/annotation-tissue-biosample/$outputPartition"

  def toOutput: String = s"biosample/$outputPartition"
  def toMergeOutput: MergeOutput = MergeOutput(outputPartition, partitionIn, partitionOut)
}

case class TissueMergeOutput(partition: String) {
  val Seq(annotation, tissue, _) = partition.split("___").toSeq
  val inputPartition: String = s"${annotation}___${tissue}___*/*.csv"
  val outputPartition: String = s"${annotation}___${tissue}"

  val partitionIn: String = s"merged/annotation-tissue-biosample/$inputPartition"
  val partitionOut: String = s"merged/annotation-tissue/$outputPartition"

  def toOutput: String = s"tissue/$outputPartition"
  def toMergeOutput: MergeOutput = MergeOutput(outputPartition, partitionIn, partitionOut)
}

object MergeOutput {
  def fromOutput(output: String): MergeOutput = {
    output.split("/").toSeq match {
      case Seq(subRegion, outputPartition) if subRegion == "biosample" =>
        BiosampleMergeOutput(s"${outputPartition}___*").toMergeOutput
      case Seq(subRegion, outputPartition) if subRegion == "tissue" =>
        TissueMergeOutput(s"${outputPartition}___*").toMergeOutput
    }
  }
}
