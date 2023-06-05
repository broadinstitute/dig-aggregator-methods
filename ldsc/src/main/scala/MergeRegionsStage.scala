package org.broadinstitute.dig.aggregator.methods.ldsc

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

class MergeRegionsStage(implicit context: Context) extends Stage {
  import Implicits.S3Key

  val partitions: Seq[String] = Seq()
  val subRegion: String = if (partitions.isEmpty) "default" else partitions.mkString("-")
  val partitionFiles: Input.Source = Input.Source.Success(s"out/ldsc/regions/${subRegion}/partitioned/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(partitionFiles)

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 200,
    applications = Seq.empty
  )

  /** The _SUCCESS file maps to a key prefix with partitioned bed files.
    *
    * Outputs will be the set of unique partition name prefixes. For example:
    *
    *   `<annotation>___<tissue>___<additional_partitions>`
    *
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case partitionFiles(dataset) => {
      val part = "/partition=([^/]+)/".r
      val partitionNames = context.s3
        .ls(s"${partitionFiles.prefix.commonPrefix}${dataset}/")
        .flatMap(obj =>
          part
            .findFirstMatchIn(obj.key)
            .map(_.group(1))
        )
        .distinct

      // if there are no partitions, then we can ignore it
      partitionNames match {
        case Nil => Outputs.Null
        case _   => Outputs.Named(partitionNames: _*)
      }
    }
  }

  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("mergeRegions.pl"), output, subRegion))
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/ldsc/regions/${subRegion}/merged/${output}/")
  }

  /** Update the success flag of the merged regions.
    */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/ldsc/regions/${subRegion}/merged/${output}/_SUCCESS")
    ()
  }
}
