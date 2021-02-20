package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class ClumpedAssociationsStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  val clumped = Input.Source.Success("out/metaanalysis/clumped/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(clumped)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case clumped(_) => Outputs.Named("clumps")
  }

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = Ec2.Strategy.generalPurpose(mem = 64.gb),
    slaveInstanceType = Ec2.Strategy.generalPurpose(mem = 64.gb),
    masterVolumeSizeInGB = 100,
    slaveVolumeSizeInGB = 100,
    instances = 5
  )

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("clumpedAssociations.py")))
  }
}
