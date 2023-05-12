package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

class MergeRegionsStage(implicit context: Context) extends Stage {
  import Implicits.S3Key

  val partitionsList: Seq[Partitions] = Seq(
    Partitions.NoPartition,
    Partitions.AnnotationTissue,
    Partitions.AnnotationTissueBiosample
  )
  val partitionFiles: Input.Source = Input.Source.Success(s"out/ldsc/regions/partitioned/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(partitionFiles)

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 200,
    applications = Seq.empty
  )

  // Makes only one call to get all of the files.
  val s3PartitionMap: Map[String, Seq[String]] = {
    val part = "/([^/]+)/partition=([^/]+)/".r
    val parts: Seq[S3Partition] = context.s3
      .ls(s"${partitionFiles.prefix.commonPrefix}").flatMap { obj =>
        part.findFirstMatchIn(obj.key)
          .map { s3Match =>
            S3Partition(s3Match.group(1), s3Match.group(2))
          }
      }.distinct
    // Create map dataset -> partitions
    parts.groupBy(_.dataset)
      .map { case (dataset, s3Partitions) =>
        dataset -> s3Partitions.map(_.partition)
      }
  }

  /** The _SUCCESS file maps to a key prefix with partitioned bed files.
    *
    * Outputs will be the set of unique partition name prefixes:
    *
    *   `<annotation>___<tissue>___<biosample>___<dataset>`
    *
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case partitionFiles(dataset) => {
      val partitionNames: Seq[String] =  s3PartitionMap.get(dataset).toSeq.flatten

      val matchedPartitions: Seq[String] = partitionsList.flatMap { partitionsToMatch =>
        partitionNames.map(MergeOutput.toMergeOutput(_, partitionsToMatch).toOutput)
      }
      // if there are no partitions, then we can ignore it
      if (matchedPartitions.nonEmpty) {
        Outputs.Named(matchedPartitions: _*)
      } else Outputs.Null
    }
  }

  /** The partition names are combined together across datasets into single
    * BED files that can then be read by GREGOR.
    */
  override def make(output: String): Job = {
    val mergeOutput = MergeOutput.fromOutputString(output)
    new Job(Job.Script(
      resourceUri("mergeRegions.pl"),
      mergeOutput.subRegion,
      mergeOutput.partitionMatch,
      mergeOutput.partitionOutput
    ))
  }

  /** No need to rm files because the same file path will be used for the output and so it will be replaced
    */

  /** Update the success flag of the merged regions.
    */
  override def success(output: String): Unit = {
    context.s3.touch(s"${MergeOutput.fromOutputString(output).outputDirectory}/_SUCCESS")
    ()
  }
}

case class S3Partition(dataset: String, partition: String)

case class Partition(name: String){
  override def toString: String = name
}

object Partition {
  val Null: Partition = Partition("null")
  val AnnotationPartition: Partition = Partition("annotation")
  val TissuePartition: Partition = Partition("tissue")
  val BiosamplePartition: Partition = Partition("biosample")
  val DatasetPartition: Partition = Partition("dataset")
}

case class Partitions(partitions: Seq[Partition]) {
  val nonNullPartitions: Seq[Partition] = partitions.filter(_ != Partition.Null)
  val subRegion: String = if (nonNullPartitions.nonEmpty) nonNullPartitions.mkString("-") else "all"

  def partitionMatch(partitionStrings: Seq[String]): String = {
    partitions.zip(partitionStrings).map {
      case (partition, partitionString) if partition != Partition.Null => partitionString
      case _ => "*"
    }.mkString("___")
  }

  def partitionOutput(partitionStrings: Seq[String]): String = {
    if (nonNullPartitions.nonEmpty) {
      partitions.zip(partitionStrings).flatMap {
        case (partition, partitionString) if partition != Partition.Null => Some(partitionString)
        case _ => None
      }.mkString("___")
    } else "all"
  }
}

object Partitions {
  val NoPartition: Partitions = Partitions(
    Seq(Partition.Null, Partition.Null, Partition.Null, Partition.Null)
  )
  val AnnotationTissue: Partitions = Partitions(
    Seq(Partition.AnnotationPartition, Partition.TissuePartition, Partition.Null, Partition.Null)
  )
  val AnnotationTissueBiosample: Partitions = Partitions(
    Seq(Partition.AnnotationPartition, Partition.TissuePartition, Partition.BiosamplePartition, Partition.Null)
  )
  val AnnotationTissueBiosampleDataset: Partitions = Partitions(
    Seq(Partition.AnnotationPartition, Partition.TissuePartition, Partition.BiosamplePartition, Partition.DatasetPartition)
  )
}

case class MergeOutput(
  subRegion: String,
  partitionMatch: String,
  partitionOutput: String
) {
  def toOutput: String = s"$subRegion/$partitionMatch/$partitionOutput"

  def outputDirectory: String = s"out/ldsc/regions/merged/$subRegion/$partitionOutput"
}

object MergeOutput {
  def toMergeOutput(partition: String, partitions: Partitions): MergeOutput = {
    val partitionStrings = partition.split("___")
    // Check that the partitions in S3 and here match in length
    assert(partitionStrings.length == partitions.partitions.length)

    MergeOutput(
      subRegion = partitions.subRegion,
      partitionMatch = partitions.partitionMatch(partitionStrings),
      partitionOutput = partitions.partitionOutput(partitionStrings)
    )
  }

  def fromOutputString(outputString: String): MergeOutput = {
    outputString.split("/").toSeq match {
      case Seq(subRegion, partitionMatch, partitionOutput) => MergeOutput(subRegion, partitionMatch, partitionOutput)
    }
  }
}
