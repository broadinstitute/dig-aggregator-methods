package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

/** The final result of all aggregator methods is building the BioIndex. All
  * outputs are to the dig-bio-index bucket in S3.
  */
class BurdenStage(implicit context: Context) extends Stage {
  val burdenResults = Input.Source.Success("out/burdenbinning/*/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(burdenResults)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case burdenResults(run_type) => Outputs.Named(run_type)
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("burden.py"), output))
  }
}
