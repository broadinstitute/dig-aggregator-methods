package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
 * outputs are to the dig-bio-index bucket in S3.
 */
class HugeStage(implicit context: Context) extends Stage {
  val common = Input.Source.Success("out/huge/common/*/")
  val rare = Input.Source.Success("out/huge/rare/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(common, rare)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case common(_) => Outputs.Named("huge")
    case rare(_) => Outputs.Named("huge")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("huge.py")))
  }
}
