package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class VariantsStage(implicit context: Context) extends Stage {
  val common: Input.Source = Input.Source.Success("out/varianteffect/common/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(common)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case common() => Outputs.Named("variants")
  }

  override val cluster: ClusterDef = {
    super.cluster.copy(
      releaseLabel = ReleaseLabel("emr-6.7.0")
    )
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("variants.py")))
  }
}
