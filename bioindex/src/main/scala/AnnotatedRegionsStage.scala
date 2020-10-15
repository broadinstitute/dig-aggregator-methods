package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class AnnotatedRegionsStage(implicit context: Context) extends Stage {
  val regions = Input.Source.Dataset("annotated_regions/")
  val tissues = Input.Source.Dataset("tissues/")

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.memoryOptimized(),
    slaveInstanceType = Ec2.Strategy.memoryOptimized(),
    instances = 5
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(regions)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("annotations")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("annotatedRegions.py")))
  }
}
