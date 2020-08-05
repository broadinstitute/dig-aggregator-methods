package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class VariantsStage(implicit context: Context) extends Stage {
  val common = Input.Source.Success("out/varianteffect/common/")

  /** Use memory-optimized machine with sizeable disk space for shuffling. */
  override val cluster: ClusterDef = super.cluster.copy(
    applications = Seq.empty,
    instances = 1
  )

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(common)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case common() => Outputs.Named("variants")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.Script(resourceUri("variants.sh")))
  }
}
