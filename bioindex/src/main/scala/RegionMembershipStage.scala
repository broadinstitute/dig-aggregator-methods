package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class RegionMembershipStage(implicit context: Context) extends Stage {
  val variants = Input.Source.Dataset("variants/*/*/*/")
  val regions: Input.Source = Input.Source.Success("out/ldsc/regions/merged/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(variants, regions)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants() => Outputs.Named("regionmembership")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("regionMembership.py")))
  }
}
