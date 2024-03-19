package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class TopAssociationsStage(implicit context: Context) extends Stage {
  val clumped: Input.Source = Input.Source.Success("out/credible_sets/merged/*/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(clumped)

  override val cluster: ClusterDef = super.cluster.copy(
    instances = 4
  )

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case clumped(_, ancestry) => Outputs.Named(ancestry)
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("topAssociations.py"), s"--ancestry=$output"))
  }
}
