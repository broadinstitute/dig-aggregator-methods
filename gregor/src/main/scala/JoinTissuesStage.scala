package org.broadinstitute.dig.aggregator.methods.gregor

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.emr._

class JoinTissuesStage(implicit context: Context) extends Stage {
  val partitions: Input.Source = Input.Source.Dataset("annotated_regions/")

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
    *   UBERON_0004222___ChromHMM___EnhancerActive1
    *   UBERON_0004222___ChromHMM___QuiescentLow
    *
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("partitions")
  }

  /** The partition names are combined together across datasets into single
    * BED files that can then be read by GREGOR.
    */
  override def make(output: String): Job = {
    val script = resourceUri("mergeRegions.sh")
    val part   = "/partition=([^/]+)/".r

    // get all the partition names
    val partitionNames = context.s3
      .ls(partitions.prefix)
      .flatMap(obj => part.findFirstMatchIn(obj.key).map(_.group(1)))
      .distinct

    // create a step per partition
    val steps = partitionNames.map(Job.Script(script, _))

    // Even though each of these jobs is a single step, all the steps can be
    // run in parallel across jobs and clusters. So, this helps improve the
    // overall performance.

    new Job(steps, parallelSteps = true)
  }

  /** Before the jobs actually run, perform this operation.
    */
  override def prepareJob(output: String): Unit = {
    context.s3.rm(s"out/gregor/regions/merged/")
  }

  /** Update the success flag of the merged regions.
    */
  override def success(output: String): Unit = {
    context.s3.touch("out/gregor/regions/merged/_SUCCESS")
    ()
  }
}
