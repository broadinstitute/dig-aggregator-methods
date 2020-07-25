package org.broadinstitute.dig.aggregator.methods.gregor

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

class MergeRegionsStage(implicit context: Context) extends Stage {
  val partitions: Input.Source = Input.Source.Success("out/gregor/regions/partitioned/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(partitions)

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
    *   UBERON_0004222___ChromHMM___EnhancerActive1
    *   UBERON_0004222___ChromHMM___QuiescentLow
    *
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case input =>
      val part = "/partition=([^/]+)/".r

      // get all the partition names
      val partitions = context.s3
        .ls(input.dirname + "partition=")
        .flatMap(obj => part.findFirstMatchIn(obj.key).map(_.group(1)))
        .distinct

      // get the unique list of all partitioned, named outputs
      Outputs.Named(partitions: _*)
  }

  /** The partition names are combined together across datasets into single
    * BED files that can then be read by GREGOR.
    */
  override def make(output: String): Job = {
    val script    = resourceUri("mergeRegions.sh")
    val partition = output

    new Job(Job.Script(script, partition))
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/gregor/regions/merged/${output}/")
  }

  /** Update the success flag of the merged regions.
    */
  override def success(output: String): Unit = {
    context.s3.touch("out/gregor/regions/merged/_SUCCESS")
    ()
  }
}
