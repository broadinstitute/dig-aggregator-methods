package org.broadinstitute.dig.aggregator.methods.ldsr

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

class MergeRegionsStage(implicit context: Context) extends Stage {
  import Implicits.S3Key

  val partitions: Input.Source = Input.Source.Success("out/ldsr/regions/partitioned/*/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(partitions)

  /** Just need a single machine with no applications, but a good drive. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 1,
    masterVolumeSizeInGB = 200,
    applications = Seq.empty,
    stepConcurrency = 5
  )

  /** The _SUCCESS file maps to a key prefix with partitioned bed files.
    *
    * Outputs will be the set of unique partition name prefixes. For example:
    *
    *   <tissue>___<annotation>___<method>
    *
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case partitions(dataset) => {
      val part = "/partition=([^/]+)/".r
      val partitionNames = context.s3
        .ls(s"${partitions.prefix.commonPrefix}${dataset}/")
        .flatMap(obj =>
          part
            .findFirstMatchIn(obj.key)
            .map(_.group(1))
        )
        .distinct

      Outputs.Named(partitionNames: _*)
    }
  }

  /** The partition names are combined together across datasets into single
    * BED files that can then be read by GREGOR.
    */
  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("mergeRegions.sh"), output))
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/ldsr/regions/merged/${output}")
  }

  /** Update the success flag of the merged regions.
    */
  override def success(output: String): Unit = {
    context.s3.touch(s"out/ldsr/regions/merged/${output}/_SUCCESS")
    ()
  }
}
