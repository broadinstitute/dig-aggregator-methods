package org.broadinstitute.dig.aggregator.methods.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.config.emr._
import org.broadinstitute.dig.aws.emr._

class CombineDiffExpStage(implicit context: Context) extends Stage {
  val diffExp: Input.Source = Input.Source.Success("diff_exp/*/", s3BucketOverride=Some(context.s3Bioindex))

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(diffExp)

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case diffExp(_) => Outputs.Named("diffExp")
  }

  /** Output to Job steps. */
  override def make(output: String): Job = {
    new Job(Job.PySpark(resourceUri("combineDiffExp.py")))
  }
}
